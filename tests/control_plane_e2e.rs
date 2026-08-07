//! Exercises live control changes against real PostgreSQL and ClickHouse
//!
//! Covers pause and reload without restart, table opt-in after catalog replay,
//! and regression where applying one table opted pinned tables out
//!
//! Skipped silently when `initdb`, `pg_basebackup`, or `clickhouse` is
//! absent. Linux only because tests use Unix sockets and POSIX data dirs

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::fs;
use std::net::SocketAddr;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use walshadow::pg::parse_pg_lsn;
use walshadow::record::WAL_SEG_SIZE;
use walshadow::shadow::{Shadow, ShadowConfig};

/// Running daemon + its source PG + CH, with the paths the tests poke.
struct Harness {
    tmp: tempfile::TempDir,
    source: Shadow,
    ch: fx::ChServer,
    child: Option<Child>,
    bin: String,
    /// Daemon argv, kept so a restart re-execs byte-identical flags
    args: Vec<String>,
    control_socket: PathBuf,
    frag_path: PathBuf,
    metrics_addr: SocketAddr,
    stderr_path: PathBuf,
    /// Second socket dir the same source cluster also listens on, so a test
    /// can move `[source] host` to a live endpoint of the same cluster
    source_sock_alt: PathBuf,
    shadow_data: PathBuf,
    shadow_sock: PathBuf,
    shadow_filter_dir: PathBuf,
    shadow_port: u16,
}

impl Harness {
    /// Bootstrap source + CH + daemon and block until the daemon's
    /// metrics port is up (bootstrap done, shadow serving, WAL pump in
    /// its main loop) and the seed row has drained to CH.
    async fn up(ports: &fx::Ports) -> Result<Self> {
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
        // Same cluster, two socket dirs: the endpoint-move drill repoints
        // `[source] host` at the second one. Last setting in the file wins.
        let source_sock_alt = tmp.path().join("source-sock-alt");
        fs::create_dir_all(&source_sock_alt).unwrap();
        {
            use std::io::Write as _;
            let conf = source.config().data_dir.join("postgresql.conf");
            let mut f = fs::OpenOptions::new().append(true).open(&conf)?;
            writeln!(
                f,
                "unix_socket_directories = '{}, {}'",
                source.config().socket_dir.display(),
                source_sock_alt.display(),
            )?;
        }
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
        // Shadow is daemon-owned, so the daemon writes the preload line itself
        // and only needs to be told where the un-installed module sits
        let pgext_dir = fx::pgext_dir();
        let stderr_path = tmp.path().join("daemon.stderr.log");
        let metrics_addr: SocketAddr = format!("127.0.0.1:{}", ports.metrics).parse().unwrap();
        let args: Vec<String> = [
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
            "--bootstrap-shadow-replay-timeout",
            "120",
            "--bridge-lib-dir",
            pgext_dir.to_str().unwrap(),
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let child = spawn_daemon(&bin, &args, &stderr_path).context("spawn walshadow-stream")?;

        let h = Harness {
            tmp,
            source,
            ch,
            child: Some(child),
            bin,
            args,
            control_socket,
            frag_path,
            metrics_addr,
            stderr_path,
            source_sock_alt,
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
        self.ctl_body(words, "")
    }

    fn ctl_body(&self, words: &[&str], body: &str) -> Result<String> {
        use std::io::Write;
        let mut child = Command::new(&self.bin)
            .arg("ctl")
            .arg("--socket")
            .arg(&self.control_socket)
            .args(words)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("spawn ctl")?;
        child
            .stdin
            .take()
            .context("ctl stdin")?
            .write_all(body.as_bytes())
            .context("write ctl body")?;
        let out = child.wait_with_output().context("ctl output")?;
        if !out.status.success() {
            bail!(
                "ctl {:?} failed: {}",
                words,
                String::from_utf8_lossy(&out.stderr)
            );
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    fn status_field(&self, key: &str) -> Result<String> {
        let body = self.ctl(&["status"])?;
        let t: toml::Table = body
            .parse()
            .with_context(|| format!("parse status toml: {body}"))?;
        let v = t
            .get(key)
            .with_context(|| format!("no {key} in status: {body}"))?;
        Ok(match v {
            toml::Value::String(s) => s.clone(),
            other => other.to_string(),
        })
    }

    /// One `ctl status` field carrying PostgreSQL LSN text, as a number.
    fn lsn_field(&self, key: &str) -> Result<u64> {
        let raw = self.status_field(key)?;
        parse_pg_lsn(&raw).with_context(|| format!("parse {key} = {raw:?}"))
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

    /// Poll one metric until it reaches `want`, then return it.
    async fn wait_metric(&self, name: &str, want: u64, timeout: Duration) -> Result<u64> {
        let deadline = Instant::now() + timeout;
        loop {
            let got = self.metric(name)?;
            if got >= want {
                return Ok(got);
            }
            if Instant::now() >= deadline {
                bail!("timeout: {name} stayed at {got}, want >= {want}");
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    /// One labelled series off `/metrics`, by its full `name{labels}` head.
    /// [`metric`](Self::metric) reads the first series of a family, which is
    /// the wrong one whenever the interesting label is not first.
    fn metric_series(&self, head: &str) -> Result<u64> {
        let body = fx::http_get(self.metrics_addr, "/metrics").context("metrics scrape")?;
        let line = body
            .lines()
            .find(|l| l.starts_with(head))
            .with_context(|| format!("no series {head} in /metrics"))?;
        line.rsplit_once(' ')
            .context("series has no value")?
            .1
            .parse()
            .with_context(|| format!("parse {line}"))
    }

    /// One counter/gauge off the daemon's `/metrics`; 0 when absent.
    fn metric(&self, name: &str) -> Result<u64> {
        let body = fx::http_get(self.metrics_addr, "/metrics").context("metrics scrape")?;
        Ok(fx::parse_metric(&body, name).unwrap_or(0))
    }

    fn psql(&self, sql: &str) -> Result<String> {
        Ok(self.source.psql_one(sql)?)
    }

    fn ch_get(&self, sql: &str) -> Result<String> {
        self.ch.query(sql)
    }

    /// Streaming standby of the source cluster: the promotion target of a
    /// switchover drill. `pg_basebackup` copies the source's
    /// `postgresql.conf`, so the target's own settings are appended after it
    /// and win by last-setting-wins. No `write_base_conf`: it pins
    /// `max_wal_senders = 0`, and the target has to serve replication both as
    /// a cascading standby and as the promoted primary.
    fn promotion_target(&self) -> Result<Shadow> {
        self.promotion_target_named("target", TARGET_PORT)
    }

    /// A second standby of the same cluster, for the sibling-branch drill: same
    /// system identifier, its own switchpoint once promoted.
    fn promotion_target_named(&self, name: &str, port: u16) -> Result<Shadow> {
        let data_dir = self.tmp.path().join(format!("{name}-data"));
        let socket_dir = self.tmp.path().join(format!("{name}-sock"));
        let filtered = self.tmp.path().join(format!("{name}-filtered"));
        fs::create_dir_all(&socket_dir).context("create target socket dir")?;
        fs::create_dir_all(&filtered).context("create target filter dir")?;

        // The harness source keeps no slot, so `-c fast`'s checkpoint recycles
        // the segment the daemon still streams from
        self.psql("ALTER SYSTEM SET wal_keep_size = '256MB'")?;
        self.psql("SELECT pg_reload_conf()")?;

        let src = self.source.config();
        let out = Command::new("pg_basebackup")
            .args([
                "-h",
                src.socket_dir.to_str().unwrap(),
                "-p",
                &src.port.to_string(),
                "-U",
                "postgres",
                "-D",
                data_dir.to_str().unwrap(),
                "-X",
                "stream",
                "-c",
                "fast",
                "--no-password",
            ])
            .output()
            .context("spawn pg_basebackup")?;
        if !out.status.success() {
            bail!("pg_basebackup: {}", String::from_utf8_lossy(&out.stderr));
        }
        {
            use std::io::Write as _;
            let conf = data_dir.join("postgresql.conf");
            let mut f = fs::OpenOptions::new()
                .append(true)
                .open(&conf)
                .context("open target postgresql.conf")?;
            writeln!(
                f,
                "\n# promotion target\n\
                 port = {port}\n\
                 unix_socket_directories = '{sock}'\n\
                 hot_standby = on\n\
                 primary_conninfo = 'host={src_sock} port={src_port} user=postgres \
                 application_name={name}'",
                port = port,
                sock = socket_dir.display(),
                src_sock = src.socket_dir.display(),
                src_port = src.port,
            )?;
        }

        let mut cfg = ShadowConfig::new(data_dir, filtered);
        cfg.port = port;
        cfg.socket_dir = socket_dir;
        cfg.ctl_timeout = Duration::from_secs(60);
        let target = Shadow::new(cfg);
        target
            .write_standby_signal()
            .context("target standby.signal")?;
        target.start().context("start promotion target")?;
        Ok(target)
    }

    fn alive(&mut self) -> bool {
        matches!(self.child.as_mut().map(|c| c.try_wait()), Some(Ok(None)))
    }

    /// Stop, then respawn with identical flags: same spill dir, same walsender
    /// bind (`SO_REUSEADDR`), no `--ignore-cursor`. The config fragment is on
    /// disk, so a restart after a switchover dials the endpoint the repoint
    /// named, resolving file-over-CLI the way boot does
    async fn restart(&mut self, ready: Duration) -> Result<()> {
        self.stop_daemon();
        self.child =
            Some(spawn_daemon(&self.bin, &self.args, &self.stderr_path).context("respawn daemon")?);
        self.wait_ready(ready).await
    }

    /// Wait until the pump loop has run once. Boot binds `/metrics` and the
    /// control socket before it resumes, so neither answering proves the pump
    /// came up; `walshadow_source_timeline` is only published from inside the
    /// loop. A daemon that exits fails here instead of at some later timeout
    async fn wait_ready(&mut self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if !self.alive() {
                bail!("daemon exited before the pump resumed");
            }
            if self.metric("walshadow_source_timeline").unwrap_or(0) > 0 {
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!("pump never resumed within {timeout:?}");
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    /// SIGKILL, no drain: what a crash leaves behind, including a shadow
    /// postmaster the daemon never got to stop.
    fn kill_daemon(&mut self) -> Result<()> {
        let mut child = self.child.take().context("daemon gone")?;
        child.kill().context("kill daemon")?;
        child.wait().context("reap daemon")?;
        Ok(())
    }

    /// Respawn after a [`kill_daemon`](Self::kill_daemon).
    async fn start_daemon(&mut self, ready: Duration) -> Result<()> {
        self.child =
            Some(spawn_daemon(&self.bin, &self.args, &self.stderr_path).context("respawn daemon")?);
        self.wait_ready(ready).await
    }

    /// Poll the daemon's stderr until `needle` shows up. The pump logs each
    /// stage of a crossing, so this is how a drill lands inside one.
    async fn wait_log(&self, needle: &str, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if self.daemon_log().contains(needle) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!("timeout: {needle:?} never logged");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// SIGINT → graceful drain so tracing flushes to the stderr file; SIGKILL
    /// only if it doesn't exit promptly.
    fn stop_daemon(&mut self) {
        let Some(mut c) = self.child.take() else {
            return;
        };
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

    /// Owned handle on the daemon-owned shadow, for a session that outlives a
    /// borrow of the harness: a blocking hold run alongside a drill.
    fn shadow(&self) -> Shadow {
        let mut cfg = ShadowConfig::new(self.shadow_data.clone(), self.shadow_filter_dir.clone());
        cfg.port = self.shadow_port;
        cfg.socket_dir = self.shadow_sock.clone();
        cfg.ctl_timeout = Duration::from_secs(60);
        Shadow::new(cfg)
    }

    /// One value out of the daemon-owned shadow, over its unix socket.
    fn shadow_psql(&self, sql: &str) -> Result<String> {
        Ok(self.shadow().psql_one(sql)?)
    }

    /// Poll shadow until `sql` equals `want`.
    async fn wait_shadow(&self, sql: &str, want: &str, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            let last = self
                .shadow_psql(sql)
                .unwrap_or_else(|_| "<query failed>".into());
            if last == want {
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!("timeout: want {want:?}, last {last:?} for `{sql}` on shadow");
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    /// Branch the shadow is on: its walreceiver's when one is attached, else
    /// the last checkpoint it read.
    fn shadow_timeline(&self) -> Result<String> {
        self.shadow_psql(
            "SELECT coalesce((SELECT received_tli FROM pg_stat_wal_receiver), timeline_id)::text \
             FROM pg_control_checkpoint()",
        )
    }

    /// Shadow's own `pg_ctl -l` log. Recovery refusals land here, not in the
    /// daemon's stderr. Message text follows the server's locale, so only the
    /// level tags are worth matching on.
    fn shadow_log(&self) -> String {
        fs::read_to_string(self.shadow_data.join("startup.log")).unwrap_or_default()
    }

    /// Daemon stderr so far. Written continuously, so a test can read it before
    /// teardown.
    fn daemon_log(&self) -> String {
        fs::read_to_string(&self.stderr_path).unwrap_or_default()
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
        self.stop_daemon();
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

/// One daemon process. Stderr appends so a restart's log lands under the
/// previous run's rather than truncating the evidence.
fn spawn_daemon(bin: &str, args: &[String], stderr_path: &Path) -> Result<Child> {
    let stderr_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(stderr_path)
        .context("open daemon stderr")?;
    Command::new(bin)
        .args(args)
        .env("RUST_LOG", "warn,walshadow=info")
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr_file))
        .process_group(0)
        .spawn()
        .context("spawn daemon")
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

const SECOND_EMAIL: &str =
    "SELECT argMax(email, _lsn) FROM demo.users WHERE _is_deleted = 0 AND id = 2";

/// Socket-only, so one fixed number per role is enough (see `common/ports.rs`).
/// Distinct from source and shadow because a drill can put all three sockets
/// under one temp dir.
const TARGET_PORT: u16 = 5434;

/// Second standby, promoted on its own to become another timeline 2.
const SIBLING_PORT: u16 = 5435;

/// `Latest checkpoint location` from a stopped cluster's `pg_control`. Fast
/// shutdown writes its checkpoint last, so this is the primary's final durable
/// record and what the promotion target must have replayed
/// (plans/failover.md §Operator protocol).
fn controldata_checkpoint_lsn(data_dir: &Path) -> Result<u64> {
    let out = Command::new("pg_controldata")
        .args(["-D", data_dir.to_str().unwrap()])
        // Labels are the parse surface; keep them English
        .env("LC_ALL", "C")
        .output()
        .context("spawn pg_controldata")?;
    if !out.status.success() {
        bail!("pg_controldata: {}", String::from_utf8_lossy(&out.stderr));
    }
    let text = String::from_utf8_lossy(&out.stdout);
    let raw = text
        .lines()
        .find_map(|l| l.strip_prefix("Latest checkpoint location:"))
        .context("no Latest checkpoint location in pg_controldata")?
        .trim();
    parse_pg_lsn(raw).with_context(|| format!("parse checkpoint location {raw:?}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_resume_via_ctl_and_sighup_no_restart() {
    if !gated() {
        return;
    }
    let mut h = Harness::up(&fx::Ports::alloc())
        .await
        .expect("bring up harness");

    let result = async {
        // Baseline: a WAL update flows to CH.
        h.psql("UPDATE demo.users SET email = 'baseline@x' WHERE id = 1")?;
        h.wait_ch(USER_EMAIL, "baseline@x", Duration::from_secs(15))
            .await?;

        // --- ctl pause/resume -------------------------------------------
        let uptime_before: u64 = h.status_field("uptime_secs")?.parse().unwrap_or(0);
        h.ctl_body(&["apply"], "[stream]\npaused = true")?;
        assert_eq!(h.status_field("paused")?, "true", "apply paused → paused");
        // API must only write its own fragment
        let frag = fs::read_to_string(&h.frag_path).context("read fragment")?;
        assert!(frag.contains("paused = true"), "fragment: {frag}");

        // Wait past idle tick so next write cannot race pause
        tokio::time::sleep(Duration::from_millis(600)).await;
        // A write made once paused has settled must not reach CH.
        h.psql("UPDATE demo.users SET email = 'while-paused@x' WHERE id = 1")?;
        h.assert_ch_stable(USER_EMAIL, "baseline@x", Duration::from_secs(5))
            .await?;

        h.ctl_body(&["apply"], "[stream]\npaused = false")?;
        assert_eq!(h.status_field("paused")?, "false", "apply resume → running");
        h.wait_ch(USER_EMAIL, "while-paused@x", Duration::from_secs(15))
            .await?;

        // Uptime catches hidden restarts
        let uptime_after: u64 = h.status_field("uptime_secs")?.parse().unwrap_or(0);
        assert!(
            uptime_after >= uptime_before,
            "uptime went backwards ({uptime_before} → {uptime_after}) — daemon restarted",
        );
        assert!(h.alive(), "daemon exited during pause/resume");

        // --- SIGHUP-triggered reload ------------------------------------
        // Fragment changes stay inactive until reload
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_table_opt_in_auto_creates_on_reload() {
    if !gated() {
        return;
    }
    let mut h = Harness::up(&fx::Ports::alloc())
        .await
        .expect("bring up harness");

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

        // CREATE may not have reached shadow catalog, trigger commits until it does
        h.ctl_body(
            &["apply"],
            "[table.demo.gizmos]\nreplicate = true\ninitial_load = \"copy\"",
        )?;
        h.ctl(&["reload"])?;
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

        // Pre-opt-in row proves copy ran
        h.wait_ch(
            "SELECT argMax(label, _lsn) FROM demo.gizmos WHERE _is_deleted = 0 AND id = 1",
            "alpha",
            Duration::from_secs(20),
        )
        .await
        .context("default backfill did not carry the pre-opt-in row")?;

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

/// Regression: applying one table used to opt pinned tables out
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn apply_preserves_previously_pinned_table() {
    if !gated() {
        return;
    }
    let mut h = Harness::up(&fx::Ports::alloc())
        .await
        .expect("bring up harness");

    let result = async {
        h.psql("UPDATE demo.users SET email = 'before-select@x' WHERE id = 1")?;
        h.wait_ch(USER_EMAIL, "before-select@x", Duration::from_secs(15))
            .await?;

        h.psql(
            "CREATE TABLE demo.gadgets (id bigint PRIMARY KEY, label text);\
             ALTER TABLE demo.gadgets REPLICA IDENTITY FULL;",
        )?;
        h.ctl_body(&["apply"], "[table.demo.gadgets]\nreplicate = true")?;
        h.ctl(&["reload"])?;

        // Unrelated apply must preserve pinned mapping
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

/// Source endpoint moves while paused, the way a switchover repoints an HA
/// address: pause, `apply [source] host`, resume. The pump swaps its feed
/// onto the new address of the same cluster and continues at the same LSN —
/// no restart, no re-bootstrap, no gap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_move_source_endpoint_resume_no_restart() {
    if !gated() {
        return;
    }
    let mut h = Harness::up(&fx::Ports::alloc())
        .await
        .expect("bring up harness");

    let result = async {
        h.psql("UPDATE demo.users SET email = 'before-move@x' WHERE id = 1")?;
        h.wait_ch(USER_EMAIL, "before-move@x", Duration::from_secs(15))
            .await?;
        let uptime_before: u64 = h.status_field("uptime_secs")?.parse().unwrap_or(0);

        h.ctl_body(&["apply"], "[stream]\npaused = true")?;
        assert_eq!(h.status_field("paused")?, "true");
        // Past the idle tick so the write below cannot race the pause
        tokio::time::sleep(Duration::from_millis(600)).await;
        h.psql("UPDATE demo.users SET email = 'while-moved@x' WHERE id = 1")?;

        // Boot's endpoint must round-trip through the resolver unchanged, or
        // every reload would redial the feed for nothing
        assert_eq!(
            h.metric("walshadow_source_endpoint_swaps_total")?,
            0,
            "feed swapped before any [source] change",
        );

        let alt = h.source_sock_alt.to_str().unwrap().to_string();
        h.ctl_body(&["apply"], &format!("[source]\nhost = \"{alt}\""))?;
        // Only the API fragment carries the move
        let frag = fs::read_to_string(&h.frag_path).context("read fragment")?;
        assert!(frag.contains(&alt), "fragment: {frag}");

        let swapped = async {
            let deadline = Instant::now() + Duration::from_secs(20);
            while Instant::now() < deadline {
                if h.metric("walshadow_source_endpoint_swaps_total")? > 0 {
                    return Ok(true);
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Ok::<bool, anyhow::Error>(false)
        }
        .await?;
        assert!(swapped, "pump never swapped onto the moved endpoint");
        assert_eq!(
            h.metric("walshadow_source_endpoint_swap_pending")?,
            0,
            "swap still pending after the feed moved",
        );

        // Resume: the frozen write drains over the new endpoint
        h.ctl_body(&["apply"], "[stream]\npaused = false")?;
        h.wait_ch(USER_EMAIL, "while-moved@x", Duration::from_secs(30))
            .await
            .context("backlog did not drain after the endpoint move")?;

        // And the stream keeps flowing from the moved address
        h.psql("UPDATE demo.users SET email = 'after-move@x' WHERE id = 1")?;
        h.wait_ch(USER_EMAIL, "after-move@x", Duration::from_secs(15))
            .await
            .context("post-move write did not reach CH")?;

        let uptime_after: u64 = h.status_field("uptime_secs")?.parse().unwrap_or(0);
        assert!(
            uptime_after >= uptime_before,
            "uptime went backwards ({uptime_before} → {uptime_after}) — daemon restarted",
        );
        assert!(h.alive(), "daemon exited during the endpoint move");
        Ok::<(), anyhow::Error>(())
    }
    .await;

    let stderr = h.teardown();
    if let Err(e) = result {
        panic!("{e:#}\n--- daemon stderr ---\n{stderr}");
    }
}

/// `[source] slot` moves with the endpoint: a promotion target names its slot
/// its own way. A name no slot answers to fails closed (swap stays pending,
/// the current feed keeps streaming); a pre-created slot is adopted at the
/// same LSN and starts holding the source's WAL.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn apply_source_slot_rename_adopts_precreated_slot() {
    if !gated() {
        return;
    }
    let mut h = Harness::up(&fx::Ports::alloc())
        .await
        .expect("bring up harness");

    let result = async {
        h.psql("UPDATE demo.users SET email = 'before-slot@x' WHERE id = 1")?;
        h.wait_ch(USER_EMAIL, "before-slot@x", Duration::from_secs(15))
            .await?;
        let failures_before = h.metric("walshadow_source_endpoint_swap_failures_total")?;

        // Walshadow creates no slot it was not booted with, so a name nothing
        // answers to leaves the boot feed streaming
        h.ctl_body(&["apply"], "[source]\nslot = \"absent_slot\"")?;
        h.wait_metric(
            "walshadow_source_endpoint_swap_failures_total",
            failures_before + 1,
            Duration::from_secs(20),
        )
        .await
        .context("swap onto a missing slot never failed")?;
        ensure!(
            h.metric("walshadow_source_endpoint_swap_pending")? == 1,
            "missing slot left no pending swap",
        );
        h.psql("UPDATE demo.users SET email = 'during-miss@x' WHERE id = 1")?;
        h.wait_ch(USER_EMAIL, "during-miss@x", Duration::from_secs(15))
            .await
            .context("boot feed stopped streaming while the swap was refused")?;

        // Pre-created, the same name is adopted at the resume LSN
        h.psql("SELECT pg_create_physical_replication_slot('absent_slot', true)")?;
        h.wait_metric(
            "walshadow_source_endpoint_swaps_total",
            1,
            Duration::from_secs(20),
        )
        .await
        .context("pump never adopted the pre-created slot")?;
        ensure!(
            h.metric("walshadow_source_endpoint_swap_pending")? == 0,
            "swap still pending after the slot was adopted",
        );

        h.psql("UPDATE demo.users SET email = 'after-slot@x' WHERE id = 1")?;
        h.wait_ch(USER_EMAIL, "after-slot@x", Duration::from_secs(15))
            .await
            .context("post-rename write did not reach CH")?;

        // Adopted means bound: the slot now pins the source's WAL
        let active =
            h.psql("SELECT active FROM pg_replication_slots WHERE slot_name = 'absent_slot'")?;
        ensure!(active == "t", "slot not streaming: {active:?}");
        ensure!(h.alive(), "daemon exited during the slot rename");
        Ok::<(), anyhow::Error>(())
    }
    .await;

    let stderr = h.teardown();
    if let Err(e) = result {
        panic!("{e:#}\n--- daemon stderr ---\n{stderr}");
    }
}

/// Spec operator procedure for consistent config change: pause, apply,
/// resume — no transaction is in flight at the apply, so every transaction
/// planned after resume (WAL backlog included) routes whole under the new
/// config. A row planned before the pause under the old config stays
/// discarded; the while-paused backlog row reroutes and lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_apply_resume_reroutes_backlog_whole() {
    if !gated() {
        return;
    }
    let mut h = Harness::up(&fx::Ports::alloc())
        .await
        .expect("bring up harness");

    let result = async {
        // Unmapped table; id=1 plans (discards) before the pause. The users
        // marker proves the reorder got past id=1's commit AND that the
        // CREATE's boundary replayed into the shadow catalog (barrier order).
        h.psql(
            "CREATE TABLE demo.widgets (id bigint PRIMARY KEY, label text);\
             ALTER TABLE demo.widgets REPLICA IDENTITY FULL;",
        )?;
        h.psql("INSERT INTO demo.widgets VALUES (1, 'pre-config')")?;
        h.psql("UPDATE demo.users SET email = 'marker1@x' WHERE id = 1")?;
        h.wait_ch(USER_EMAIL, "marker1@x", Duration::from_secs(15))
            .await?;
        assert_eq!(
            h.ch_get("EXISTS TABLE demo.widgets")?,
            "0",
            "widgets must not exist on CH before opt-in",
        );

        h.ctl_body(&["apply"], "[stream]\npaused = true")?;
        assert_eq!(h.status_field("paused")?, "true");
        // Wait past idle tick so the next write cannot race the pause
        tokio::time::sleep(Duration::from_millis(600)).await;

        // Backlog row frozen in WAL, unplanned; then the opt-in applies
        // while nothing is in flight.
        h.psql("INSERT INTO demo.widgets VALUES (2, 'while-paused')")?;
        h.ctl_body(&["apply"], "[table.demo.widgets]\nreplicate = true")?;
        h.ctl(&["reload"])?;

        h.ctl_body(&["apply"], "[stream]\npaused = false")?;
        assert_eq!(h.status_field("paused")?, "false");

        // Backlog xact plans after resume → routes under the new config.
        h.wait_ch(
            "SELECT argMax(label, _lsn) FROM demo.widgets WHERE _is_deleted = 0 AND id = 2",
            "while-paused",
            Duration::from_secs(30),
        )
        .await
        .context("while-paused backlog row did not reroute after resume")?;

        // Post-resume stream continues under the same config.
        h.psql("INSERT INTO demo.widgets VALUES (3, 'post-resume')")?;
        h.wait_ch(
            "SELECT argMax(label, _lsn) FROM demo.widgets WHERE _is_deleted = 0 AND id = 3",
            "post-resume",
            Duration::from_secs(15),
        )
        .await?;

        // id=1 was planned pre-pause under the old config: discarded stays
        // discarded, no partial re-plan.
        let n = h.ch_get("SELECT count() FROM demo.widgets FINAL WHERE _is_deleted = 0")?;
        assert_eq!(n, "2", "exactly the backlog + post-resume rows land");

        assert!(h.alive(), "daemon exited during pause/apply/resume");
        Ok::<(), anyhow::Error>(())
    }
    .await;

    let stderr = h.teardown();
    if let Err(e) = result {
        panic!("{e:#}\n--- daemon stderr ---\n{stderr}");
    }
}

/// Pause must publish the frontier it froze, so a promotion decision reads a
/// value that cannot move under it (plans/failover.md §Surfaces).
/// `pause_consumed_lsn` is what resume asks the promoted target to
/// serve; `pause_received_lsn` is the source head the target must reach first.
/// Both stay put while the pipeline keeps draining behind them
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_publishes_frozen_frontier_lsns() {
    if !gated() {
        return;
    }
    let mut h = Harness::up(&fx::Ports::alloc())
        .await
        .expect("bring up harness");

    let result = async {
        h.psql("UPDATE demo.users SET email = 'before-pause@x' WHERE id = 1")?;
        h.wait_ch(USER_EMAIL, "before-pause@x", Duration::from_secs(15))
            .await?;

        h.ctl_body(&["apply"], "[stream]\npaused = true")?;
        assert_eq!(h.status_field("paused")?, "true");
        // Past the idle tick, so the pump has observed the pause and frozen
        tokio::time::sleep(Duration::from_millis(600)).await;

        let consumed = h.lsn_field("pause_consumed_lsn")?;
        let received = h.lsn_field("pause_received_lsn")?;
        assert!(consumed > 0, "frozen consumed frontier at 0");
        assert!(
            consumed <= received,
            "consumed {consumed:X} above received {received:X}",
        );
        assert_eq!(
            h.metric("walshadow_pause_consumed_lsn")?,
            consumed,
            "metric disagrees with ctl status",
        );
        assert_eq!(
            h.metric("walshadow_pause_received_lsn")?,
            received,
            "metric disagrees with ctl status",
        );

        // Source keeps writing and the pipeline keeps draining. Neither moves
        // a frontier frozen at the pause
        h.psql("UPDATE demo.users SET email = 'while-paused@x' WHERE id = 1")?;
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(
            h.lsn_field("pause_consumed_lsn")?,
            consumed,
            "pause_consumed_lsn moved while paused",
        );
        assert_eq!(
            h.lsn_field("pause_received_lsn")?,
            received,
            "pause_received_lsn moved while paused",
        );

        h.ctl_body(&["apply"], "[stream]\npaused = false")?;
        h.wait_ch(USER_EMAIL, "while-paused@x", Duration::from_secs(30))
            .await
            .context("backlog did not drain after resume")?;
        assert!(h.alive(), "daemon exited");
        Ok::<(), anyhow::Error>(())
    }
    .await;

    let stderr = h.teardown();
    if let Err(e) = result {
        panic!("{e:#}\n--- daemon stderr ---\n{stderr}");
    }
}

/// Newest version of one row, without FINAL so a merge cannot hide a stale
/// duplicate.
fn email_of(id: u32) -> String {
    format!("SELECT argMax(email, _lsn) FROM demo.users WHERE _is_deleted = 0 AND id = {id}")
}

/// Steps 2 to 7 of plans/failover.md §Operator protocol against standby
/// `target`, leaving the pump to cross the fork on its own. Row id 2 commits
/// below the fork while walshadow is paused, so the drain from the frozen
/// frontier to `F` owes it to ClickHouse
async fn drive_switchover(h: &Harness, target: &Shadow) -> Result<()> {
    promote_and_repoint(h, target).await?;
    // 6. Resume: ancestor drains to the fork, then the descendant serves
    h.ctl_body(&["apply"], "[stream]\npaused = false")?;
    Ok(())
}

/// Steps 2 to 6: everything through the promotion, leaving the pump paused
/// below a fork it has not been told about
async fn promote_and_repoint(h: &Harness, target: &Shadow) -> Result<()> {
    pause_and_stop_writes(h, target).await?;
    repoint(h, target, 1).await?;
    promote(target)?;
    Ok(())
}

/// Steps 2 and 3, plus step 5's replay proof taken by hand: pause below the
/// fork, stop writes, prove the target holds the whole tail. `[source]` still
/// names the stopped primary at the end of it
async fn pause_and_stop_writes(h: &Harness, target: &Shadow) -> Result<()> {
    assert_eq!(
        target.psql_one("SELECT pg_is_in_recovery()")?,
        "t",
        "promotion target is not a standby",
    );
    // 1. WAL path live before the pause. `Harness::up` returns as soon as the
    //    bootstrap backfill lands the seed row, which is before the pump's first
    //    START_REPLICATION — pausing and stopping the source ahead of that races
    //    boot instead of drilling a switchover
    h.psql("UPDATE demo.users SET email = 'pre-switchover@x' WHERE id = 1")?;
    h.wait_ch(USER_EMAIL, "pre-switchover@x", Duration::from_secs(30))
        .await
        .context("pump never streamed a WAL-delivered row")?;

    // 2. Pause freezes the consumed frontier below a fork that does not exist yet
    h.ctl_body(&["apply"], "[stream]\npaused = true")?;
    assert_eq!(h.status_field("paused")?, "true");
    tokio::time::sleep(Duration::from_millis(600)).await;

    // 3. A paused walshadow sends no standby status, so its walsender only exits
    //    on `wal_sender_timeout` and fast shutdown waits for that
    h.psql("INSERT INTO demo.users VALUES (2, 'bob', 'below-fork@x')")?;
    h.psql("ALTER SYSTEM SET wal_sender_timeout = '5s'")?;
    h.psql("SELECT pg_reload_conf()")?;
    h.source.stop().context("stop source primary")?;

    // 4. Prove the target replayed the primary's final durable record, and that
    //    nothing it received sits unapplied
    let checkpoint = controldata_checkpoint_lsn(&h.source.config().data_dir)?;
    let replayed = target
        .wait_for_replay(checkpoint, Duration::from_secs(30))
        .context("target never replayed the shutdown checkpoint")?;
    let received = parse_pg_lsn(&target.psql_one("SELECT pg_last_wal_receive_lsn()")?)
        .context("target receive lsn")?;
    assert_eq!(
        received, replayed,
        "target received {received:X} but replayed {replayed:X}",
    );
    Ok(())
}

/// Step 4, before the promotion: the crossing then arrives as the walsender's
/// next-timeline result on the connection walshadow already holds
async fn repoint(h: &Harness, target: &Shadow, swaps: u64) -> Result<()> {
    apply_repoint(h, target)?;
    h.wait_metric(
        "walshadow_source_endpoint_swaps_total",
        swaps,
        Duration::from_secs(30),
    )
    .await
    .context("pump never swapped onto the promotion target")?;
    ensure!(
        h.status_field("source_swap_pending")? == "false",
        "swap still pending, promotion would go out from under the pump",
    );
    Ok(())
}

/// Step 4 without waiting for the pump to reach it: a daemon still waiting for
/// its source at boot adopts the moved endpoint there instead of swapping.
fn apply_repoint(h: &Harness, target: &Shadow) -> Result<()> {
    let target_sock = target.config().socket_dir.display().to_string();
    h.ctl_body(
        &["apply"],
        &format!("[source]\nhost = \"{target_sock}\"\nport = {TARGET_PORT}"),
    )?;
    Ok(())
}

/// Switchpoint the promotion wrote into the descendant's history file, read off
/// the target because a drill has to act on the fork before walshadow has
/// proved it
fn fork_switch_lsn(target: &Shadow, timeline: u32) -> Result<String> {
    let file = format!("pg_wal/{timeline:08X}.history");
    let lsn = target.psql_one(&format!(
        "SELECT split_part(pg_read_file('{file}'), E'\\t', 2)"
    ))?;
    parse_pg_lsn(&lsn).with_context(|| format!("switchpoint in {file}: {lsn:?}"))?;
    Ok(lsn)
}

/// Park the shadow's startup process on the ancestor's last record. Recovery
/// pause is honoured before the timeline rescan, so the end-of-timeline the
/// crossing sends next cannot move the shadow off the branch, while the
/// walreceiver keeps answering status so the fork barrier still reads the apply
/// position it waits on
fn pause_at_fork_sql(switch_lsn: &str) -> String {
    format!(
        "DO $$
         DECLARE deadline timestamptz := clock_timestamp() + interval '90 seconds';
         BEGIN
           WHILE pg_last_wal_replay_lsn() < '{switch_lsn}'::pg_lsn LOOP
             IF clock_timestamp() > deadline THEN
               RAISE EXCEPTION 'shadow never replayed to the fork at {switch_lsn}';
             END IF;
             PERFORM pg_sleep(0.002);
           END LOOP;
           PERFORM pg_wal_replay_pause();
         END $$"
    )
}

fn promote(target: &Shadow) -> Result<()> {
    ensure!(
        target.psql_one("SELECT pg_promote(true, 60)")? == "t",
        "pg_promote",
    );
    ensure!(
        target.psql_one("SELECT pg_is_in_recovery()")? == "f",
        "target still in recovery after promote",
    );
    Ok(())
}

/// Operator-driven switchover, plans/failover.md §Operator protocol, on a
/// slotless run: pause, stop writes with `-m fast`, prove the target holds the
/// whole tail, repoint `[source]` while it is still a standby, promote, resume.
/// Walshadow must drain the ancestor timeline to the fork, cross to the
/// descendant, and lose no committed row — no `--ignore-cursor`, no restart,
/// no rebootstrap
///
/// Step 5's gate is taken by hand here, off the stopped primary's shutdown
/// checkpoint; `status_answers_the_promotion_gate_before_the_promotion` drills
/// the `ctl status` form of the same three terms
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn switchover_crosses_fork_and_keeps_every_row() {
    if !gated() {
        return;
    }
    let mut h = Harness::up(&fx::Ports::alloc())
        .await
        .expect("bring up harness");
    // Seed row in CH proves bootstrap finished. Cloning the source mid-bootstrap
    // truncates the daemon's own base backup ("tar unpack: unexpected EOF")
    h.wait_ch(USER_EMAIL, "alice@seed", Duration::from_secs(30))
        .await
        .expect("seed row backfills");
    // Outside the drill body so it gets stopped even when an assert fires
    let target = h.promotion_target().expect("build promotion target");

    let result = async {
        drive_switchover(&h, &target).await?;
        h.wait_ch(SECOND_EMAIL, "below-fork@x", Duration::from_secs(60))
            .await
            .context("row committed below the fork never reached CH")?;

        target.psql_one("UPDATE demo.users SET email = 'after-promote@x' WHERE id = 1")?;
        h.wait_ch(USER_EMAIL, "after-promote@x", Duration::from_secs(60))
            .await
            .context("write on the descendant timeline never reached CH")?;

        assert!(
            h.metric("walshadow_timeline_switches_total")? >= 1,
            "no timeline switch recorded",
        );
        assert_eq!(
            h.metric("walshadow_source_timeline")?,
            2,
            "pump still reading the ancestor",
        );
        assert!(
            h.metric("walshadow_timeline_prefix_bytes_verified_total")? > 0,
            "fork prefix was never verified against the ancestor",
        );

        // Shadow has to follow the same chain, or its catalog stops answering
        // for the branch ClickHouse is being filled from
        h.wait_shadow(
            "SELECT received_tli FROM pg_stat_wal_receiver",
            "2",
            Duration::from_secs(60),
        )
        .await
        .context("shadow never streamed the descendant timeline")?;
        // Crossing on the wire, not through a rediscovery: the walsender finished
        // the ancestor branch with the next-timeline result and the walreceiver
        // came back for the descendant
        assert!(
            h.daemon_log()
                .contains("historic timeline served to its switchpoint"),
            "shadow crossed some other way:\n{}",
            h.daemon_log(),
        );
        // Message text is localized, the level tag is not
        let log = h.shadow_log();
        assert!(!log.contains("PANIC"), "shadow log carries a PANIC:\n{log}");
        assert!(h.alive(), "daemon exited crossing the fork");
        Ok::<(), anyhow::Error>(())
    }
    .await;

    // Consumer before producer: walshadow caps advertised flush at its durable
    // floor, so its walsender never confirms everything sent and the target's
    // fast shutdown would sit out `wal_sender_timeout` waiting for it
    let stderr = h.teardown();
    let _ = target.stop();
    if let Err(e) = result {
        panic!("{e:#}\n--- daemon stderr ---\n{stderr}");
    }
}

/// A crossing has to survive a restart, on both sides of the fork
/// (plans/failover.md §Crossing order). Same drill as
/// `switchover_crosses_fork_and_keeps_every_row`, then two restarts:
///
/// - floor at the fork segment's start, which the barrier commits on the
///   descendant while it still sits *below* the fork LSN. Boot has to resolve
///   that branch per segment, ask the source for a descendant-named segment it
///   fills with the ancestor prefix, and open a descriptor log whose header
///   still names the ancestor
/// - floor past the fork, once descendant WAL has moved the natural terms up
///
/// Neither restart may need `--ignore-cursor`, a rebootstrap, or a shadow
/// rebuild: every artifact is valid for the branch the floor sits on. Nor may
/// either see the mixed state the barrier deletes — a walsender advertising the
/// ancestor is refused outright by a shadow already past `F`
/// (`src/backend/replication/walreceiver.c`: *highest timeline %u of the primary
/// is behind recovery timeline %u*), which closes during startup without ever
/// sending `START_REPLICATION`
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_after_switchover_resumes_on_both_sides_of_the_fork() {
    if !gated() {
        return;
    }
    let mut h = Harness::up(&fx::Ports::alloc())
        .await
        .expect("bring up harness");
    h.wait_ch(USER_EMAIL, "alice@seed", Duration::from_secs(30))
        .await
        .expect("seed row backfills");
    let target = h.promotion_target().expect("build promotion target");

    let result = async {
        drive_switchover(&h, &target).await?;
        h.wait_ch(SECOND_EMAIL, "below-fork@x", Duration::from_secs(60))
            .await
            .context("row committed below the fork never reached CH")?;
        h.wait_metric(
            "walshadow_timeline_switches_total",
            1,
            Duration::from_secs(30),
        )
        .await
        .context("pump never crossed the fork")?;
        // Captured now: the counters are per-process, so a restart zeroes them
        let switch_lsn = h.metric("walshadow_timeline_switch_lsn")?;
        assert!(switch_lsn > 0, "crossing reported no fork LSN");
        // The shadow must be past the fork too, else the first restart is not
        // exercising the window
        h.wait_shadow(
            "SELECT received_tli FROM pg_stat_wal_receiver",
            "2",
            Duration::from_secs(60),
        )
        .await
        .context("shadow never streamed the descendant timeline")?;

        // The barrier's commit: floor at the fork segment's start, named on the
        // descendant. The fork segment cannot have sealed — a promotion plus one
        // small write does not fill a segment — so this position is reachable
        // only because a fork copies the ancestor prefix into a descendant-named
        // file, not because the natural floor terms got there
        let floor = h.lsn_field("floor")?;
        anyhow::ensure!(
            floor == switch_lsn - switch_lsn % WAL_SEG_SIZE,
            "floor {floor:X} is not the fork segment's start for {switch_lsn:X}",
        );
        anyhow::ensure!(floor <= switch_lsn, "floor {floor:X} past the fork");
        anyhow::ensure!(
            h.status_field("floor_timeline")? == "2",
            "floor sits in the fork segment, so it is the descendant's file, \
             but reads as {:?}",
            h.status_field("floor_timeline")?,
        );

        h.restart(Duration::from_secs(90))
            .await
            .context("restart with the floor at the fork segment's start")?;
        // The floor's own branch is the descendant while the fork it crossed is
        // still below it, so boot owes the shadow that switchpoint again — a
        // shadow killed before the advertisement has nowhere else to learn it
        ensure!(
            h.daemon_log()
                .contains("re-advertising a crossing the floor already made"),
            "boot left the walsender with no switchpoint below the floor:\n{}",
            h.daemon_log(),
        );
        // Survives the restart because it comes off the source's history chain,
        // not off a per-process counter
        assert_eq!(
            h.metric("walshadow_timeline_switch_lsn")?,
            switch_lsn,
            "fork LSN did not come back from the timeline history",
        );
        target.psql_one("INSERT INTO demo.users VALUES (3, 'carol', 'after-restart@x')")?;
        h.wait_ch(&email_of(3), "after-restart@x", Duration::from_secs(90))
            .await
            .context("nothing flowed after the fork-segment-floor restart")?;
        h.wait_shadow(
            "SELECT received_tli FROM pg_stat_wal_receiver",
            "2",
            Duration::from_secs(60),
        )
        .await
        .context("shadow never got back onto the descendant after restart")?;

        // Lift the floor clear of the fork: a switched segment seals the fork
        // segment (`filter_durable`) and an acked commit in the next one lifts
        // `align_down(emitter_ack)`, the two terms of the resolved floor
        target.psql_one("SELECT pg_switch_wal()")?;
        target.psql_one("UPDATE demo.users SET email = 'past-fork@x' WHERE id = 1")?;
        h.wait_ch(USER_EMAIL, "past-fork@x", Duration::from_secs(60))
            .await
            .context("post-switch write never reached CH")?;
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let floor = h.lsn_field("floor")?;
            if floor > switch_lsn {
                break;
            }
            if Instant::now() >= deadline {
                bail!("floor stalled at {floor:X}, never crossed the fork {switch_lsn:X}");
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        assert_eq!(h.status_field("floor_timeline")?, "2");

        h.restart(Duration::from_secs(90))
            .await
            .context("restart with the floor past the fork")?;
        target.psql_one("INSERT INTO demo.users VALUES (4, 'dave', 'descendant-floor@x')")?;
        h.wait_ch(&email_of(4), "descendant-floor@x", Duration::from_secs(90))
            .await
            .context("nothing flowed after the descendant-floor restart")?;

        let log = h.shadow_log();
        assert!(!log.contains("PANIC"), "shadow log carries a PANIC:\n{log}");
        assert!(h.alive(), "daemon exited after the restarts");
        Ok::<(), anyhow::Error>(())
    }
    .await;

    let stderr = h.teardown();
    let _ = target.stop();
    if let Err(e) = result {
        panic!("{e:#}\n--- daemon stderr ---\n{stderr}");
    }
}

/// Restart after the promotion, before the crossing: the floor still names the
/// ancestor, while the fork segment holding it is already the descendant's file.
/// Resolving that branch by segment alone would boot straight onto the
/// descendant, so nothing walks the ancestor to its end — the shadow keeps
/// replaying a branch no walsender will finish for it, and no history file ever
/// places the descendant it has to move to
/// (plans/failover.md §Lineage)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_between_promotion_and_crossing_still_crosses() {
    if !gated() {
        return;
    }
    let mut h = Harness::up(&fx::Ports::alloc())
        .await
        .expect("bring up harness");
    h.wait_ch(USER_EMAIL, "alice@seed", Duration::from_secs(30))
        .await
        .expect("seed row backfills");
    let target = h.promotion_target().expect("build promotion target");

    let result = async {
        promote_and_repoint(&h, &target).await?;
        h.restart(Duration::from_secs(90))
            .await
            .context("restart between the promotion and the crossing")?;
        ensure!(
            h.status_field("paused")? == "true",
            "restart lost the pause the operator took the promotion decision under",
        );
        ensure!(
            h.metric("walshadow_source_timeline")? == 1,
            "boot adopted the descendant with the floor still on the ancestor, \
             so the crossing it owes is gone",
        );

        h.ctl_body(&["apply"], "[stream]\npaused = false")?;
        h.wait_metric(
            "walshadow_timeline_switches_total",
            1,
            Duration::from_secs(60),
        )
        .await
        .context("pump never crossed the fork after the restart")?;
        h.wait_ch(SECOND_EMAIL, "below-fork@x", Duration::from_secs(60))
            .await
            .context("row committed below the fork never reached CH")?;
        target.psql_one("UPDATE demo.users SET email = 'after-promote@x' WHERE id = 1")?;
        h.wait_ch(USER_EMAIL, "after-promote@x", Duration::from_secs(60))
            .await
            .context("write on the descendant timeline never reached CH")?;
        h.wait_shadow(
            "SELECT received_tli FROM pg_stat_wal_receiver",
            "2",
            Duration::from_secs(60),
        )
        .await
        .context("shadow never streamed the descendant timeline")?;

        let log = h.shadow_log();
        ensure!(!log.contains("PANIC"), "shadow log carries a PANIC:\n{log}");
        ensure!(h.alive(), "daemon exited crossing the fork after a restart");
        Ok::<(), anyhow::Error>(())
    }
    .await;

    let stderr = h.teardown();
    let _ = target.stop();
    if let Err(e) = result {
        panic!("{e:#}\n--- daemon stderr ---\n{stderr}");
    }
}

/// Step 5's gate answered by `ctl status` instead of a second `psql`: with the
/// repoint made, walshadow already holds a connection to the target, so it can
/// report the target's replay and receive positions beside the frozen frontier
/// they have to reach, plus one ready / not-ready that names the failing term
/// (plans/failover.md §Operator protocol)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_answers_the_promotion_gate_before_the_promotion() {
    if !gated() {
        return;
    }
    let mut h = Harness::up(&fx::Ports::alloc())
        .await
        .expect("bring up harness");
    h.wait_ch(USER_EMAIL, "alice@seed", Duration::from_secs(30))
        .await
        .expect("seed row backfills");
    // The gate is published from inside the pump loop, so the backfill landing
    // is not enough to read it
    h.wait_ready(Duration::from_secs(60))
        .await
        .expect("pump never started");
    let target = h.promotion_target().expect("build promotion target");

    let result = async {
        ensure!(
            h.status_field("promotion_blocked_on")? == "not_paused",
            "an unpaused pump has no frozen frontier to gate against",
        );
        pause_and_stop_writes(&h, &target).await?;
        // `[source]` still names the old primary, which is no standby — and now
        // not even up
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let blocked = h.status_field("promotion_blocked_on")?;
            if blocked == "source_unreachable" || blocked == "not_a_standby" {
                break;
            }
            ensure!(
                Instant::now() < deadline,
                "gate never named the stopped primary, said {blocked:?}",
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        ensure!(h.status_field("promotion_ready")? == "false");

        repoint(&h, &target, 1).await?;
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if h.status_field("promotion_ready")? == "true" {
                break;
            }
            ensure!(
                Instant::now() < deadline,
                "gate never opened: {:?}",
                h.status_field("promotion_blocked_on")?,
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        ensure!(h.status_field("target_in_recovery")? == "true");
        let replay = h.lsn_field("target_replay_lsn")?;
        let receive = h.lsn_field("target_receive_lsn")?;
        let frozen = h.lsn_field("pause_received_lsn")?;
        ensure!(
            replay >= frozen,
            "gate opened with replay {replay:X} below the frozen head {frozen:X}",
        );
        ensure!(
            receive <= replay,
            "gate opened with {} bytes received but unapplied",
            receive - replay,
        );

        promote(&target)?;
        // Promotion closes the gate again: the target answers for itself now
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if h.status_field("promotion_blocked_on")? == "not_a_standby" {
                break;
            }
            ensure!(
                Instant::now() < deadline,
                "gate stayed open past a promotion"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        h.ctl_body(&["apply"], "[stream]\npaused = false")?;
        h.wait_ch(SECOND_EMAIL, "below-fork@x", Duration::from_secs(60))
            .await
            .context("row committed below the fork never reached CH")?;
        ensure!(
            h.status_field("promotion_blocked_on")? == "not_paused",
            "a resumed pump still answers a promotion gate",
        );
        ensure!(h.alive(), "daemon exited");
        Ok::<(), anyhow::Error>(())
    }
    .await;

    let stderr = h.teardown();
    let _ = target.stop();
    if let Err(e) = result {
        panic!("{e:#}\n--- daemon stderr ---\n{stderr}");
    }
}

/// Restart while paused below the fork, which is where the operator takes the
/// promotion decision. The pause survives, the frontier re-freezes rather than
/// coming back stale, and `ctl status` says so — the pair read before the
/// restart has to be read again (plans/failover.md §What pause freezes)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_while_paused_refreezes_the_frontier_and_still_crosses() {
    if !gated() {
        return;
    }
    let mut h = Harness::up(&fx::Ports::alloc())
        .await
        .expect("bring up harness");
    h.wait_ch(USER_EMAIL, "alice@seed", Duration::from_secs(30))
        .await
        .expect("seed row backfills");
    let target = h.promotion_target().expect("build promotion target");

    let result = async {
        pause_and_stop_writes(&h, &target).await?;
        ensure!(
            h.status_field("pause_refrozen")? == "false",
            "this process watched the pause arrive",
        );
        let consumed = h.lsn_field("pause_consumed_lsn")?;

        // The source is down and `[source]` still names it, so boot waits for
        // the repoint rather than exiting into a supervisor loop
        h.stop_daemon();
        h.start_daemon(Duration::from_secs(8))
            .await
            .expect_err("pump must not resume while the source it is pointed at is stopped");
        ensure!(
            h.alive(),
            "daemon exited instead of waiting for the source to come back",
        );
        // Boot adopts the moved endpoint where it is waiting, so this repoint
        // lands as a boot resolution rather than as a feed swap
        apply_repoint(&h, &target)?;
        h.wait_ready(Duration::from_secs(60))
            .await
            .context("pump never resumed after the repoint")?;

        ensure!(
            h.status_field("paused")? == "true",
            "restart lost the pause the promotion decision is taken under",
        );
        ensure!(
            h.status_field("pause_refrozen")? == "true",
            "a pause this process found already in effect re-froze silently",
        );
        let refrozen = h.lsn_field("pause_consumed_lsn")?;
        ensure!(refrozen > 0, "re-frozen consumed frontier reads as 0/0");
        ensure!(
            refrozen <= consumed,
            "re-freeze moved the consumed frontier up, to {refrozen:X} from {consumed:X}",
        );

        promote(&target)?;
        h.ctl_body(&["apply"], "[stream]\npaused = false")?;
        h.wait_ch(SECOND_EMAIL, "below-fork@x", Duration::from_secs(60))
            .await
            .context("row committed below the fork never reached CH")?;
        target.psql_one("UPDATE demo.users SET email = 'after-promote@x' WHERE id = 1")?;
        h.wait_ch(USER_EMAIL, "after-promote@x", Duration::from_secs(60))
            .await
            .context("write on the descendant timeline never reached CH")?;
        ensure!(h.metric("walshadow_source_timeline")? == 2);
        ensure!(h.alive(), "daemon exited crossing the fork");
        Ok::<(), anyhow::Error>(())
    }
    .await;

    let stderr = h.teardown();
    let _ = target.stop();
    if let Err(e) = result {
        panic!("{e:#}\n--- daemon stderr ---\n{stderr}");
    }
}

/// Restart inside the barrier: the crossing has proved the fork and is waiting
/// for the pipeline to reach it, so nothing durable names the descendant yet.
/// The restart has to come back on the ancestor, re-cross, and arrive at the
/// same barrier (plans/failover.md §Barrier)
///
/// The barrier is held open by pausing the shadow's replay, which is one of its
/// three terms. Restarting restarts the shadow too, which clears the pause, so
/// the second attempt runs the whole crossing
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_inside_the_fork_barrier_recrosses_and_converges() {
    if !gated() {
        return;
    }
    let mut h = Harness::up(&fx::Ports::alloc())
        .await
        .expect("bring up harness");
    h.wait_ch(USER_EMAIL, "alice@seed", Duration::from_secs(30))
        .await
        .expect("seed row backfills");
    let target = h.promotion_target().expect("build promotion target");

    let result = async {
        // After the pre-switchover row has drained: a shadow paused below it
        // never lets that row reach ClickHouse, and the drill needs the pause
        // to bite at the fork rather than before it
        promote_and_repoint(&h, &target).await?;
        h.shadow_psql("SELECT pg_wal_replay_pause()")
            .context("pause shadow replay")?;
        h.ctl_body(&["apply"], "[stream]\npaused = false")?;
        h.wait_log("fork barrier", Duration::from_secs(60))
            .await
            .context("crossing never reached the barrier")?;
        ensure!(
            h.metric("walshadow_timeline_switches_total")? == 0,
            "barrier opened with the shadow's replay paused",
        );
        ensure!(
            h.status_field("floor_timeline")? == "1",
            "floor named the descendant before the commit",
        );

        h.restart(Duration::from_secs(90))
            .await
            .context("restart inside the barrier")?;
        h.wait_metric(
            "walshadow_timeline_switches_total",
            1,
            Duration::from_secs(90),
        )
        .await
        .context("pump never re-crossed the fork after the restart")?;
        h.wait_ch(SECOND_EMAIL, "below-fork@x", Duration::from_secs(60))
            .await
            .context("row committed below the fork never reached CH")?;
        target.psql_one("UPDATE demo.users SET email = 'after-promote@x' WHERE id = 1")?;
        h.wait_ch(USER_EMAIL, "after-promote@x", Duration::from_secs(60))
            .await
            .context("write on the descendant timeline never reached CH")?;
        h.wait_shadow(
            "SELECT received_tli FROM pg_stat_wal_receiver",
            "2",
            Duration::from_secs(60),
        )
        .await
        .context("shadow never streamed the descendant timeline")?;
        h.wait_metric(
            "walshadow_shadow_replay_timeline",
            2,
            Duration::from_secs(60),
        )
        .await
        .context("shadow replay timeline metric never reached the descendant")?;

        let log = h.shadow_log();
        ensure!(!log.contains("PANIC"), "shadow log carries a PANIC:\n{log}");
        ensure!(h.alive(), "daemon exited");
        Ok::<(), anyhow::Error>(())
    }
    .await;

    let stderr = h.teardown();
    let _ = target.stop();
    if let Err(e) = result {
        panic!("{e:#}\n--- daemon stderr ---\n{stderr}");
    }
}

/// The one window where two branches coexist: the resume position is committed
/// on the descendant while the shadow still sits at `F` on the ancestor. Boot
/// has to re-advertise the switchpoint the crossing never delivered, and the
/// shadow has to cross on that seeded list alone
/// (plans/failover.md §Crossing)
///
/// A shadow told about the descendant crosses on its own within milliseconds of
/// the commit, so the window is held open rather than raced for: recovery is
/// paused the instant the ancestor's last record is replayed, which is the same
/// position the fork barrier waits on
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_between_the_commit_and_the_advertise_crosses_on_the_seeded_list() {
    if !gated() {
        return;
    }
    let mut h = Harness::up(&fx::Ports::alloc())
        .await
        .expect("bring up harness");
    h.wait_ch(USER_EMAIL, "alice@seed", Duration::from_secs(30))
        .await
        .expect("seed row backfills");
    let target = h.promotion_target().expect("build promotion target");

    let result = async {
        promote_and_repoint(&h, &target).await?;
        let hold = tokio::task::spawn_blocking({
            let shadow = h.shadow();
            let sql = pause_at_fork_sql(&fork_switch_lsn(&target, 2)?);
            move || shadow.psql_one(&sql)
        });
        // 6. Resume: ancestor drains to the fork, then the descendant serves
        h.ctl_body(&["apply"], "[stream]\npaused = false")?;
        hold.await
            .context("join the hold at the fork")?
            .context("hold the shadow at the fork")?;
        h.wait_shadow(
            "SELECT pg_get_wal_replay_pause_state()",
            "paused",
            Duration::from_secs(30),
        )
        .await
        .context("shadow never parked on the ancestor's last record")?;
        h.wait_log(
            "committed the fork resume position",
            Duration::from_secs(60),
        )
        .await
        .context("crossing never committed a resume position")?;
        h.kill_daemon()?;
        ensure!(
            h.shadow_timeline()? != "2",
            "shadow crossed before the kill, so this is not the window under test",
        );

        h.start_daemon(Duration::from_secs(120))
            .await
            .context("restart between the commit and the advertise")?;
        ensure!(
            h.status_field("floor_timeline")? == "2",
            "boot did not resume on the branch the crossing committed",
        );
        ensure!(
            h.daemon_log()
                .contains("re-advertising a crossing the floor already made"),
            "boot left the walsender with no switchpoint for the branch the \
             shadow is still on:\n{}",
            h.daemon_log(),
        );
        h.wait_shadow(
            "SELECT received_tli FROM pg_stat_wal_receiver",
            "2",
            Duration::from_secs(90),
        )
        .await
        .context("shadow never crossed on the seeded switchpoint")?;
        h.wait_ch(SECOND_EMAIL, "below-fork@x", Duration::from_secs(90))
            .await
            .context("row committed below the fork never reached CH")?;
        target.psql_one("UPDATE demo.users SET email = 'after-promote@x' WHERE id = 1")?;
        h.wait_ch(USER_EMAIL, "after-promote@x", Duration::from_secs(90))
            .await
            .context("write on the descendant timeline never reached CH")?;

        let log = h.shadow_log();
        ensure!(!log.contains("PANIC"), "shadow log carries a PANIC:\n{log}");
        ensure!(h.alive(), "daemon exited");
        Ok::<(), anyhow::Error>(())
    }
    .await;

    let stderr = h.teardown();
    let _ = target.stop();
    if let Err(e) = result {
        panic!("{e:#}\n--- daemon stderr ---\n{stderr}");
    }
}

/// Slot mode across a switchover: the target's slot is the operator's to
/// create, and walshadow proves it rather than creating one at the target's
/// head and calling that continuation successful. A slot the target does not
/// have refuses the repoint by name, and one that goes missing between the
/// repoint and the fork parks the crossing instead of exiting the daemon into a
/// restart loop (plans/failover.md §Refusals)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slot_proofs_name_the_missing_slot_and_park_the_crossing() {
    if !gated() {
        return;
    }
    let mut h = Harness::up(&fx::Ports::alloc())
        .await
        .expect("bring up harness");
    h.wait_ch(USER_EMAIL, "alice@seed", Duration::from_secs(30))
        .await
        .expect("seed row backfills");
    h.wait_ready(Duration::from_secs(60))
        .await
        .expect("pump never started");
    let target = h.promotion_target().expect("build promotion target");

    let result = async {
        // Protocol step 1: the slot exists on both ends before anything moves.
        // Slots are neither copied by a base backup nor synchronized to a
        // standby, so the target's is created on the target
        h.psql("SELECT pg_create_physical_replication_slot('sw', true)")?;
        h.ctl_body(&["apply"], "[source]\nslot = \"sw\"")?;
        h.wait_metric(
            "walshadow_source_endpoint_swaps_total",
            1,
            Duration::from_secs(30),
        )
        .await
        .context("pump never bound the source slot")?;
        target.psql_one("SELECT pg_create_physical_replication_slot('sw', true)")?;

        pause_and_stop_writes(&h, &target).await?;
        repoint(&h, &target, 2).await?;
        ensure!(
            h.status_field("source_swap_blocked_on")?.is_empty(),
            "repoint onto a target holding the slot reported a refusal",
        );
        promote(&target)?;

        // A slot the target never had: named, not left as "the swap failed"
        let failures = h.metric("walshadow_source_endpoint_swap_failures_total")?;
        h.ctl_body(&["apply"], "[source]\nslot = \"sw_gone\"")?;
        h.wait_metric(
            "walshadow_source_endpoint_swap_failures_total",
            failures + 1,
            Duration::from_secs(20),
        )
        .await
        .context("swap onto a slot the target lacks never failed")?;
        ensure!(
            h.status_field("source_swap_blocked_on")? == "slot_missing",
            "swap refusal read as {:?}",
            h.status_field("source_swap_blocked_on")?,
        );

        // The crossing reads the live slot name too, so it refuses the same way
        // — and parks, keeping ctl and /metrics answering
        h.ctl_body(&["apply"], "[stream]\npaused = false")?;
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if h.metric("walshadow_crossing_wedged")? == 1 {
                break;
            }
            ensure!(h.alive(), "daemon exited on a refused crossing");
            ensure!(Instant::now() < deadline, "crossing never parked");
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        ensure!(
            h.status_field("crossing_blocked_on")? == "slot_missing",
            "parked crossing read as {:?}",
            h.status_field("crossing_blocked_on")?,
        );
        ensure!(
            h.status_field("crossing_detail")?.contains("sw_gone"),
            "parked crossing did not name the slot: {:?}",
            h.status_field("crossing_detail")?,
        );
        ensure!(
            h.metric_series("walshadow_timeline_switch_failures_total{reason=\"slot_missing\"}")?
                >= 1,
            "refusal went uncounted",
        );
        ensure!(
            h.metric("walshadow_timeline_switches_total")? == 0,
            "a parked crossing counted as a switch",
        );
        ensure!(
            h.status_field("floor_timeline")? == "1",
            "a parked crossing moved the floor onto the descendant",
        );

        // Operator fixes what the reason named, then takes the crossing
        // decision back and gives it again
        h.ctl_body(&["apply"], "[source]\nslot = \"sw\"")?;
        h.ctl_body(&["apply"], "[stream]\npaused = true")?;
        tokio::time::sleep(Duration::from_millis(600)).await;
        ensure!(
            h.metric("walshadow_crossing_wedged")? == 0,
            "pause left the crossing parked",
        );
        h.ctl_body(&["apply"], "[stream]\npaused = false")?;
        h.wait_metric(
            "walshadow_timeline_switches_total",
            1,
            Duration::from_secs(60),
        )
        .await
        .context("crossing never ran after the slot came back")?;
        h.wait_ch(SECOND_EMAIL, "below-fork@x", Duration::from_secs(60))
            .await
            .context("row committed below the fork never reached CH")?;
        target.psql_one("UPDATE demo.users SET email = 'after-promote@x' WHERE id = 1")?;
        h.wait_ch(USER_EMAIL, "after-promote@x", Duration::from_secs(60))
            .await
            .context("write on the descendant timeline never reached CH")?;
        let active =
            target.psql_one("SELECT active FROM pg_replication_slots WHERE slot_name = 'sw'")?;
        ensure!(active == "t", "descendant slot not streaming: {active:?}");
        ensure!(h.alive(), "daemon exited");
        Ok::<(), anyhow::Error>(())
    }
    .await;

    let stderr = h.teardown();
    let _ = target.stop();
    if let Err(e) = result {
        panic!("{e:#}\n--- daemon stderr ---\n{stderr}");
    }
}

/// A stable endpoint — VIP, proxy, or here one unix socket path — moves the
/// server without moving the address, so there is no repoint to tell walshadow
/// the source forked. The reconnect has to notice on its own: same cluster,
/// live timeline newer than the one being read, the read branch still serving
/// the resume LSN, so it asks for that branch and lets the walsender end it at
/// the fork (plans/failover.md §Operator protocol)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promotion_under_a_stable_endpoint_crosses_without_a_repoint() {
    if !gated() {
        return;
    }
    let mut h = Harness::up(&fx::Ports::alloc())
        .await
        .expect("bring up harness");
    h.wait_ch(USER_EMAIL, "alice@seed", Duration::from_secs(30))
        .await
        .expect("seed row backfills");
    let target = h.promotion_target().expect("build promotion target");
    let mut moved: Option<Shadow> = None;

    let result = async {
        pause_and_stop_writes(&h, &target).await?;
        // The address `[source]` names is free now, so the target takes it
        moved = Some(take_over_source_address(&h, &target)?);
        let at_source_address = moved.as_ref().expect("just moved");
        promote(at_source_address)?;

        h.ctl_body(&["apply"], "[stream]\npaused = false")?;
        h.wait_metric(
            "walshadow_timeline_switches_total",
            1,
            Duration::from_secs(90),
        )
        .await
        .context("reconnect onto the promoted endpoint never crossed the fork")?;
        ensure!(
            h.metric("walshadow_source_endpoint_swaps_total")? == 0,
            "the drill repointed, which is the case this one is not",
        );
        h.wait_ch(SECOND_EMAIL, "below-fork@x", Duration::from_secs(60))
            .await
            .context("row committed below the fork never reached CH")?;
        at_source_address
            .psql_one("UPDATE demo.users SET email = 'after-promote@x' WHERE id = 1")?;
        h.wait_ch(USER_EMAIL, "after-promote@x", Duration::from_secs(60))
            .await
            .context("write on the descendant timeline never reached CH")?;
        h.wait_shadow(
            "SELECT received_tli FROM pg_stat_wal_receiver",
            "2",
            Duration::from_secs(60),
        )
        .await
        .context("shadow never streamed the descendant timeline")?;
        ensure!(h.metric("walshadow_source_timeline")? == 2);
        h.wait_metric(
            "walshadow_shadow_replay_timeline",
            2,
            Duration::from_secs(60),
        )
        .await
        .context("shadow replay timeline metric never reached the descendant")?;
        let log = h.shadow_log();
        ensure!(!log.contains("PANIC"), "shadow log carries a PANIC:\n{log}");
        ensure!(h.alive(), "daemon exited crossing the fork");
        Ok::<(), anyhow::Error>(())
    }
    .await;

    let stderr = h.teardown();
    if let Some(m) = moved {
        let _ = m.stop();
    } else {
        let _ = target.stop();
    }
    if let Err(e) = result {
        panic!("{e:#}\n--- daemon stderr ---\n{stderr}");
    }
}

/// Restart the promotion target on the stopped primary's socket path and port,
/// so `[source]` reaches it without being touched. Last setting wins in
/// `postgresql.conf`, so appending is enough.
fn take_over_source_address(h: &Harness, target: &Shadow) -> Result<Shadow> {
    use std::io::Write as _;
    target.stop().context("stop target before it moves")?;
    let src = h.source.config();
    let data_dir = target.config().data_dir.clone();
    {
        let conf = data_dir.join("postgresql.conf");
        let mut f = fs::OpenOptions::new().append(true).open(&conf)?;
        writeln!(
            f,
            "\n# stable endpoint: the address the old primary answered on\n\
             port = {port}\n\
             unix_socket_directories = '{sock}'",
            port = src.port,
            sock = src.socket_dir.display(),
        )?;
    }
    let mut cfg = ShadowConfig::new(data_dir, target.config().filter_out_dir.clone());
    cfg.port = src.port;
    cfg.socket_dir = src.socket_dir.clone();
    cfg.ctl_timeout = Duration::from_secs(60);
    let moved = Shadow::new(cfg);
    moved
        .start()
        .context("start target at the source address")?;
    Ok(moved)
}

/// Timeline numbers are not unique across branches: two standbys of one primary,
/// promoted independently, are both timeline 2 under one system identifier. The
/// chain places either of them, so only where the branch begins separates the
/// one walshadow crossed onto from its sibling (plans/failover.md §Lineage)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sibling_timeline_two_is_refused_against_its_switchpoint() {
    if !gated() {
        return;
    }
    let mut h = Harness::up(&fx::Ports::alloc())
        .await
        .expect("bring up harness");
    h.wait_ch(USER_EMAIL, "alice@seed", Duration::from_secs(30))
        .await
        .expect("seed row backfills");
    let target = h.promotion_target().expect("build promotion target");
    let sibling = h
        .promotion_target_named("sibling", SIBLING_PORT)
        .expect("build sibling standby");
    // Stopped before the switchover's writes, so its own promotion forks below
    // the fork walshadow crosses
    sibling.stop().expect("stop sibling below the fork");

    let result = async {
        drive_switchover(&h, &target).await?;
        h.wait_metric(
            "walshadow_timeline_switches_total",
            1,
            Duration::from_secs(60),
        )
        .await
        .context("pump never crossed onto the promotion target")?;
        h.wait_ch(SECOND_EMAIL, "below-fork@x", Duration::from_secs(60))
            .await
            .context("row committed below the fork never reached CH")?;
        ensure!(h.status_field("source_timeline")? == "2");

        // The sibling comes up with no primary to stream from and promotes at
        // the end of what it holds, which is its own switchpoint
        sibling.start().context("start sibling")?;
        promote(&sibling)?;
        let swaps = h.metric("walshadow_source_endpoint_swaps_total")?;
        let sibling_sock = sibling.config().socket_dir.display().to_string();
        h.ctl_body(
            &["apply"],
            &format!("[source]\nhost = \"{sibling_sock}\"\nport = {SIBLING_PORT}"),
        )?;
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if h.status_field("source_swap_blocked_on")? == "sibling_branch" {
                break;
            }
            ensure!(
                Instant::now() < deadline,
                "sibling was not refused as one; swap says {:?}",
                h.status_field("source_swap_blocked_on")?,
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        ensure!(
            h.metric("walshadow_source_endpoint_swaps_total")? == swaps,
            "pump adopted a branch the chain it came in on cannot place",
        );
        ensure!(
            h.metric_series("walshadow_timeline_switch_failures_total{reason=\"sibling_branch\"}")?
                >= 1,
            "refusal went uncounted",
        );

        // Refused means the feed it already had stays up
        target.psql_one("UPDATE demo.users SET email = 'still-on-target@x' WHERE id = 1")?;
        h.wait_ch(USER_EMAIL, "still-on-target@x", Duration::from_secs(60))
            .await
            .context("refusing the sibling stopped the stream that was working")?;
        ensure!(h.alive(), "daemon exited refusing a sibling");
        Ok::<(), anyhow::Error>(())
    }
    .await;

    let stderr = h.teardown();
    let _ = target.stop();
    let _ = sibling.stop();
    if let Err(e) = result {
        panic!("{e:#}\n--- daemon stderr ---\n{stderr}");
    }
}
