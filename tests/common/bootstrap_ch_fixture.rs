//! Shared scaffolding for the bootstrap → CH end-to-end
//! drills (`bootstrap_direct_ch.rs`,
//! `bootstrap_object_store_ch.rs`).
//!
//! Owns: the `ChServer` subprocess wrapper (lifted from
//! `pipeline_e2e.rs` so the pipeline DDL drill, both bootstrap drills,
//! and the kill-restart drill share one driver), TOML CH-config
//! rendering for the table mapping the daemon consumes via
//! `--ch-config`, and the `assert_ch_matches_source` count/sum/md5
//! oracle the two drills share.
//!
//! Included from the test files via `#[path = "common/..."]` rather
//! than wired through `tests/common/mod.rs` because Cargo would
//! otherwise build `common` as a free-standing test binary.

#![allow(dead_code)]

use std::fs;
use std::io::Write;
use std::net::TcpStream;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use walshadow::mapping::TableTarget;
use walshadow::schema::RelName;
use walshadow::shadow::Shadow;

/// ClickHouse server subprocess wrapper shared by the pipeline DDL
/// drill, both bootstrap-to-CH drills, and the
/// kill-restart drill.
pub struct ChServer {
    child: Child,
    pub port: u16,
    pub http_port: u16,
    #[allow(dead_code)]
    tmp: tempfile::TempDir,
}

impl ChServer {
    pub fn spawn(tmp: tempfile::TempDir, tcp_port: u16, http_port: u16) -> Result<Self> {
        let data_dir = tmp.path().join("ch");
        fs::create_dir_all(&data_dir)?;
        let log_dir = tmp.path().join("ch-logs");
        fs::create_dir_all(&log_dir)?;
        let child = Command::new("clickhouse")
            .args([
                "server",
                "--",
                &format!("--tcp_port={tcp_port}"),
                &format!("--http_port={http_port}"),
                &format!("--interserver_http_port={}", http_port + 1),
                "--mysql_port=",
                "--postgresql_port=",
                "--grpc_port=",
                "--prometheus.port=",
                "--listen_host=127.0.0.1",
                &format!("--path={}/", data_dir.display()),
                &format!("--logger.log={}/server.log", log_dir.display()),
                &format!("--logger.errorlog={}/error.log", log_dir.display()),
                "--logger.level=warning",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()
            .context("spawn clickhouse server")?;
        let s = Self {
            child,
            port: tcp_port,
            http_port,
            tmp,
        };
        s.wait_for_listen(Duration::from_secs(60))?;
        Ok(s)
    }

    fn wait_for_listen(&self, deadline: Duration) -> Result<()> {
        let start = Instant::now();
        let addr = format!("127.0.0.1:{}", self.port);
        while start.elapsed() < deadline {
            if TcpStream::connect_timeout(&addr.parse().unwrap(), Duration::from_millis(200))
                .is_ok()
                && self.query("SELECT 1").is_ok()
            {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        bail!("clickhouse server failed to accept queries within {deadline:?}");
    }

    pub fn query(&self, sql: &str) -> Result<String> {
        let out = Command::new("clickhouse")
            .args([
                "client",
                "--host",
                "127.0.0.1",
                "--port",
                &self.port.to_string(),
                "--query",
                sql,
            ])
            .output()
            .context("spawn clickhouse client")?;
        if !out.status.success() {
            bail!(
                "clickhouse query failed: {} (stderr={})",
                sql,
                String::from_utf8_lossy(&out.stderr)
            );
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }
}

impl Drop for ChServer {
    fn drop(&mut self) {
        let _ = Command::new("clickhouse")
            .args([
                "client",
                "--host",
                "127.0.0.1",
                "--port",
                &self.port.to_string(),
                "--query",
                "SYSTEM SHUTDOWN",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        for _ in 0..50 {
            match self.child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) => std::thread::sleep(Duration::from_millis(100)),
                Err(_) => break,
            }
        }
        let pgid = self.child.id() as i32;
        let _ = Command::new("kill")
            .args(["-KILL", &format!("-{pgid}")])
            .stderr(Stdio::null())
            .status();
        let _ = self.child.wait();
    }
}

/// Skip-gate probe — same shape as `pipeline_e2e.rs::clickhouse_available`.
pub fn clickhouse_available() -> bool {
    Command::new("clickhouse")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

pub fn pg_available() -> bool {
    Command::new("initdb")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

pub fn pg_basebackup_available() -> bool {
    Command::new("pg_basebackup")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Write a `--ch-config` TOML mapping `<schema>.t (id int4, name text)`
/// onto `<database>.t` on CH. Mirrors the synthetic column shape the
/// emitter advertises (`_lsn` / `_xid` / `_commit_ts` / `_is_deleted`).
pub fn write_ch_config_toml(
    path: &Path,
    ch_host: &str,
    ch_port: u16,
    ch_database: &str,
    source_table: &RelName,
    target_table: &TableTarget,
) -> Result<()> {
    let body = format!(
        "[ch]\n\
         host = \"{ch_host}\"\n\
         port = {ch_port}\n\
         database = \"{ch_database}\"\n\
         compression = \"lz4\"\n\
         \n\
         [table.\"{src_ns}\".\"{src_name}\"]\n\
         target_database = \"{tgt_db}\"\n\
         target_table = \"{tgt_table}\"\n\
         columns = [\n  \
           {{ attnum = 1, target = \"id\",   type = \"Int32\"  }},\n  \
           {{ attnum = 2, target = \"name\", type = \"String\" }},\n\
         ]\n",
        src_ns = source_table.namespace,
        src_name = source_table.name,
        tgt_db = target_table.database,
        tgt_table = target_table.table,
    );
    fs::write(path, body).with_context(|| format!("write ch-config {}", path.display()))?;
    Ok(())
}

/// CREATE TABLE on CH matching the mapping above. The synthetic
/// trailer (`_lsn` / `_xid` / `_commit_ts` / `_is_deleted`) matches the
/// shape the daemon's emitter advertises in its INSERT block; the
/// engine is `ReplacingMergeTree(_lsn)` so reads with `FINAL` collapse
/// the bootstrap row to its newest copy if a later WAL update lands.
pub fn create_ch_dest_table(ch: &ChServer, database: &str, table: &str) -> Result<()> {
    ch.query(&format!("CREATE DATABASE IF NOT EXISTS {database}"))?;
    ch.query(&format!(
        "CREATE OR REPLACE TABLE {database}.{table} (\
            id Int32,\
            name String,\
            _lsn UInt64,\
            _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id"
    ))?;
    Ok(())
}

/// Append `wal_level=logical` + `max_wal_senders` to the source PG's
/// postgresql.conf so the daemon's preflight (which insists on
/// `logical`) clears and BASE_BACKUP can attach. Mirrors the
/// `append_source_conf` helpers in the bootstrap drill tests.
pub fn append_source_conf(sh: &Shadow) -> Result<()> {
    let path = sh.config().data_dir.join("postgresql.conf");
    let mut f = fs::OpenOptions::new().append(true).open(&path)?;
    writeln!(f, "\n# walshadow bootstrap-CH source overrides")?;
    writeln!(f, "wal_level = logical")?;
    writeln!(f, "max_wal_senders = 4")?;
    Ok(())
}

/// Load `<schema>.t (id int4, name text)` with `n_rows` deterministic
/// rows; CHECKPOINT + pg_switch_wal so the bootstrap pulls a stable
/// heap page snapshot.
pub fn load_source_workload(source: &Shadow, schema: &str, n_rows: i32) -> Result<()> {
    let sql = format!(
        "CREATE SCHEMA {schema};\n\
         CREATE TABLE {schema}.t (id int4 PRIMARY KEY, name text NOT NULL);\n\
         ALTER TABLE {schema}.t REPLICA IDENTITY FULL;\n\
         INSERT INTO {schema}.t \
           SELECT g, 'row-'||g::text FROM generate_series(1, {n_rows}) g;\n\
         CHECKPOINT;\n\
         SELECT pg_switch_wal();\n",
    );
    source.apply_schema_dump(&sql)?;
    Ok(())
}

/// HTTP/1.0 GET against a localhost endpoint, returns the body.
/// Hand-rolled to avoid pulling a reqwest-class dep into dev-deps.
pub fn http_get(addr: std::net::SocketAddr, path: &str) -> Result<String> {
    use std::io::{Read as _, Write as _};
    let mut sock = TcpStream::connect_timeout(&addr, Duration::from_secs(2))
        .with_context(|| format!("connect {addr}"))?;
    sock.set_read_timeout(Some(Duration::from_secs(5)))?;
    write!(sock, "GET {path} HTTP/1.0\r\nHost: localhost\r\n\r\n")?;
    let mut buf = Vec::new();
    sock.read_to_end(&mut buf)?;
    let txt = String::from_utf8_lossy(&buf).into_owned();
    let body_start = txt.find("\r\n\r\n").map(|i| i + 4).unwrap_or(0);
    Ok(txt[body_start..].to_string())
}

/// Parse a single Prometheus gauge / counter value out of a `/metrics`
/// body. Returns `None` when absent or unparseable. Matches `name <v>`
/// or `name{label=...} <v>`.
pub fn parse_metric(body: &str, name: &str) -> Option<u64> {
    for line in body.lines() {
        if line.starts_with('#') {
            continue;
        }
        let head = line.split_once(' ').map(|(h, _)| h)?;
        let stem = head.split_once('{').map(|(s, _)| s).unwrap_or(head);
        if stem != name {
            continue;
        }
        let value_str = line.rsplit_once(' ').map(|(_, v)| v)?;
        if let Ok(v) = value_str.parse::<u64>() {
            return Some(v);
        }
        if let Ok(v) = value_str.parse::<f64>() {
            return Some(v as u64);
        }
    }
    None
}

/// Poll a TCP port until accept succeeds. Used as a coarse readiness
/// gate for `walshadow-stream`'s metrics endpoint — by the time it's
/// listening, bootstrap + autospawn-shadow + WAL pump init have all
/// finished and bootstrap rows have drained to CH.
pub fn wait_for_listen(addr: std::net::SocketAddr, deadline: Duration) -> Result<()> {
    let start = Instant::now();
    while start.elapsed() < deadline {
        if TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    bail!("nothing listening on {addr} after {deadline:?}");
}

/// Drive a child to exit, polling every 100 ms until `deadline`. Hard-
/// kills + reaps on timeout so a stuck daemon doesn't outlive the
/// test.
pub fn wait_with_timeout(
    child: &mut Child,
    deadline: Duration,
) -> Result<std::process::ExitStatus> {
    let start = Instant::now();
    while start.elapsed() < deadline {
        match child.try_wait()? {
            Some(s) => return Ok(s),
            None => std::thread::sleep(Duration::from_millis(100)),
        }
    }
    let _ = child.kill();
    let _ = child.wait();
    bail!("walshadow-stream did not exit within {deadline:?}");
}

/// RAII wrapper that SIGKILLs the walshadow-stream subprocess on drop
/// (test failure path). Tests that own a clean exit consume the child
/// via `wait_with_timeout` first.
pub struct ChildGuard {
    pub child: Option<Child>,
}

impl ChildGuard {
    pub fn new(child: Child) -> Self {
        Self { child: Some(child) }
    }

    pub fn into_inner(mut self) -> Option<Child> {
        self.child.take()
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

/// Copy a test's tempdir into `$WALSHADOW_ARTIFACT_DIR/<label>/` so CI
/// can upload it on failure. No-op when the env var is unset (local
/// runs). Skips PG data-dir bulk (`pg_wal`, `base`, etc.) and the raw
/// filtered WAL segments — keeps `log/postmaster.log`, `core.*`, `*.conf`,
/// `*.opts`, signals, daemon stderr, spill files, segment manifests.
/// Per-file cap of 64 MiB as a safety net. Errors only log — artifact
/// dump must not mask the original failure.
pub fn dump_artifacts(src: &std::path::Path, label: &str) {
    let Ok(root) = std::env::var("WALSHADOW_ARTIFACT_DIR") else {
        return;
    };
    let dest = std::path::PathBuf::from(root).join(label);
    if let Err(e) = copy_dir_filtered(src, &dest, 64 * 1024 * 1024) {
        eprintln!("dump_artifacts({label}): {e}");
    } else {
        eprintln!("dump_artifacts({label}): wrote {}", dest.display());
    }
}

/// Directory names skipped wholesale inside a PG cluster data dir or
/// next to it. Mostly heap pages + xact state — useless for diagnosing
/// daemon / shadow behaviour, and big enough to blow the runner's
/// artifact quota (pgbench-scale source-data alone is ~150 MB).
const SKIP_DIR_NAMES: &[&str] = &[
    "base",
    "global",
    "pg_wal",
    "pg_xact",
    "pg_subtrans",
    "pg_multixact",
    "pg_commit_ts",
    "pg_dynshmem",
    "pg_notify",
    "pg_replslot",
    "pg_serial",
    "pg_snapshots",
    "pg_stat",
    "pg_stat_tmp",
    "pg_tblspc",
    "pg_twophase",
    "pg_logical",
];

fn copy_dir_filtered(
    src: &std::path::Path,
    dst: &std::path::Path,
    per_file_cap: u64,
) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let name = entry.file_name();
        let src_p = entry.path();
        let dst_p = dst.join(&name);
        if ty.is_dir() {
            if SKIP_DIR_NAMES
                .iter()
                .any(|s| std::ffi::OsStr::new(s) == name)
            {
                continue;
            }
            copy_dir_filtered(&src_p, &dst_p, per_file_cap)?;
            continue;
        }
        // PG's `source-sock/.s.PGSQL.*` and shadow's lock files would
        // surface as unix sockets / fifos — `fs::copy` returns ENXIO on
        // those. Only regular files have content worth uploading.
        if !ty.is_file() {
            continue;
        }
        // `filtered/` carries 16 MiB WAL segments alongside their
        // `*.manifest.json` companions. Manifests describe per-record
        // filter decisions — keep them; segments are recoverable from
        // source on demand and just inflate the artifact.
        let nstr = name.to_string_lossy();
        if (nstr.starts_with("00000001") || nstr.starts_with("0000000"))
            && !nstr.contains("manifest")
        {
            continue;
        }
        let meta = entry.metadata()?;
        if meta.len() > per_file_cap {
            std::fs::write(
                dst_p.with_extension("skipped"),
                format!("skipped: size {} > cap {}\n", meta.len(), per_file_cap),
            )?;
            continue;
        }
        // Best-effort per-file: log and continue on copy failure so a
        // single weird file (permissions, race against PG shutdown)
        // doesn't truncate the whole dump.
        if let Err(e) = std::fs::copy(&src_p, &dst_p) {
            eprintln!("dump_artifacts: skip {}: {e}", src_p.display());
        }
    }
    Ok(())
}

/// Drop guard that calls `Shadow::stop` on the wrapped fixture. Lets
/// each test own a single owner of the PG lifecycle even when the
/// fixture leaks via panic.
pub struct StopOnDrop<'a> {
    pub sh: &'a Shadow,
}

impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        let _ = self.sh.stop();
    }
}

/// Count + sum + md5(string_agg(name, ',' ORDER BY id)) oracle. Same
/// three numbers extracted from both sides; mismatch surfaces as a
/// detailed panic with both halves' values.
///
/// CH side reads `FINAL WHERE _is_deleted = 0` to collapse any
/// ReplacingMergeTree duplicates (a follow-up WAL UPDATE would
/// otherwise show twice).
pub fn assert_ch_matches_source(
    ch: &ChServer,
    source: &Shadow,
    src_table: &str,
    ch_table: &str,
) -> Result<()> {
    let src_count = source.psql_one(&format!("SELECT count(*) FROM {src_table}"))?;
    let src_sum = source.psql_one(&format!(
        "SELECT coalesce(sum(id), 0)::text FROM {src_table}"
    ))?;
    let src_md5 = source.psql_one(&format!(
        "SELECT md5(string_agg(name, ',' ORDER BY id)) FROM {src_table}"
    ))?;

    let ch_count = ch.query(&format!(
        "SELECT count() FROM {ch_table} FINAL WHERE _is_deleted = 0"
    ))?;
    let ch_sum = ch.query(&format!(
        "SELECT sum(id) FROM {ch_table} FINAL WHERE _is_deleted = 0"
    ))?;
    // CH's `lower(hex(MD5(...)))` matches PG's `md5(text)` byte-for-byte.
    let ch_md5 = ch.query(&format!(
        "SELECT lower(hex(MD5(arrayStringConcat(groupArray(name), ',')))) \
         FROM (SELECT name FROM {ch_table} FINAL WHERE _is_deleted = 0 ORDER BY id)"
    ))?;

    if src_count != ch_count {
        bail!("row count mismatch: source={src_count}, ch={ch_count}");
    }
    if src_sum != ch_sum {
        bail!("sum(id) mismatch: source={src_sum}, ch={ch_sum}");
    }
    if src_md5 != ch_md5 {
        bail!(
            "md5(string_agg(name)) mismatch: source={src_md5}, ch={ch_md5} \
             (count={src_count}, sum={src_sum})"
        );
    }
    Ok(())
}
