//! Shadow Postgres lifecycle.
//!
//! Owns the co-located Postgres instance walshadow uses as catalog
//! mirror & decode oracle. Wraps PG binaries (`initdb`, `pg_ctl`,
//! `psql`) plus on-disk plumbing (`postgresql.conf`, `standby.signal`,
//! `restore_command`) into a small struct.
//!
//! Typical bootstrap:
//!
//! 1. [`Shadow::initdb`] — bootstrap empty cluster
//! 2. [`Shadow::write_base_conf`] — append walshadow knobs to
//!    `postgresql.conf` (port, socket dir, autovacuum off, …)
//! 3. [`Shadow::start`] — start in normal mode for schema restore
//! 4. [`Shadow::apply_schema_dump`] — pipe a `pg_dump --schema-only`
//!    payload via `psql -f -`
//! 5. [`Shadow::stop`] — clean shutdown
//! 6. [`Shadow::enable_standby_recovery`] — write `standby.signal` &
//!    append `restore_command` pointing at the filter output dir
//! 7. [`Shadow::start`] — restart in standby (recovery) mode
//! 8. [`Shadow::wait_for_replay`] — block on
//!    `pg_is_in_recovery() AND pg_last_wal_replay_lsn() >= target`
//! 9. [`Shadow::health`] — periodic catalog probe
//!
//! Step 4 stays a primitive: walshadow does not reach out to source PG
//! here (daemon orchestration owns that). Callers feed in the dump
//! string they obtained however they prefer.
//!
//! Standby signal vs recovery signal: a naive setup says
//! `recovery.signal`, but §Architecture describes shadow as a
//! *standby* — `recovery.signal` exits recovery when archive WAL runs
//! out, which is the wrong primitive. `standby.signal` keeps the
//! cluster in continuous recovery, retrying `restore_command` on each
//! new segment landed by the filter. This module writes
//! `standby.signal`.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ShadowError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("{cmd} failed (exit {status}): {stderr}")]
    Process {
        cmd: String,
        status: i32,
        stderr: String,
    },
    #[error("psql parse: {0}")]
    PsqlParse(String),
    #[error("timeout waiting for {what} after {elapsed:?}")]
    Timeout { what: String, elapsed: Duration },
    #[error("shadow not in recovery (probe returned f)")]
    NotInRecovery,
    #[error("missing required PG binary: {0}")]
    MissingBinary(String),
}

pub type Result<T> = std::result::Result<T, ShadowError>;

#[derive(Debug, Clone)]
pub struct ShadowConfig {
    /// PG data directory (`-D` to all binaries).
    pub data_dir: PathBuf,
    /// TCP port shadow listens on. Connections still go over the unix
    /// socket; `listen_addresses` stays empty.
    pub port: u16,
    /// Directory shadow exposes its unix socket in.
    pub socket_dir: PathBuf,
    /// Directory walshadow's filter writes filtered segments into.
    /// Source path for shadow's `restore_command`.
    pub filter_out_dir: PathBuf,
    /// Override location for PG binaries (`initdb`, `pg_ctl`, `psql`).
    /// `None` → resolved via `$PATH`.
    pub pg_bin_dir: Option<PathBuf>,
    /// Per-`pg_ctl` `-t` timeout, used for both start and stop.
    pub ctl_timeout: Duration,
    /// [`Shadow::wait_for_replay`] poll interval.
    pub wait_poll: Duration,
}

impl ShadowConfig {
    pub fn new(data_dir: PathBuf, filter_out_dir: PathBuf) -> Self {
        let socket_dir = data_dir
            .parent()
            .unwrap_or_else(|| Path::new("/tmp"))
            .join("shadow_sock");
        Self {
            data_dir,
            port: 55434,
            socket_dir,
            filter_out_dir,
            pg_bin_dir: None,
            ctl_timeout: Duration::from_secs(60),
            wait_poll: Duration::from_millis(200),
        }
    }

    fn bin(&self, name: &str) -> PathBuf {
        match &self.pg_bin_dir {
            Some(d) => d.join(name),
            None => PathBuf::from(name),
        }
    }

    fn data_str(&self) -> &str {
        self.data_dir.to_str().expect("non-utf8 data_dir")
    }

    fn socket_str(&self) -> &str {
        self.socket_dir.to_str().expect("non-utf8 socket_dir")
    }
}

/// One-shot snapshot of shadow's recovery & catalog state. Used for
/// liveness / lag monitoring and for spot-checking that catalog replay
/// produced a sane `pg_class`.
#[derive(Debug, Clone)]
pub struct HealthReport {
    pub in_recovery: bool,
    /// `pg_last_wal_replay_lsn()` — `None` when shadow has not yet
    /// replayed any WAL (normal-mode startup or just-promoted standby).
    pub replay_lsn: Option<u64>,
    pub pg_class_count: u64,
    /// `relname` for the `pg_proc`-oid row in `pg_class`. Always
    /// `"pg_proc"` on a healthy cluster — surfaces catalog corruption
    /// or replay-LSN-pinned-far-behind in a single probe.
    pub pg_proc_relname: String,
}

pub struct Shadow {
    config: ShadowConfig,
}

impl Shadow {
    pub fn new(config: ShadowConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &ShadowConfig {
        &self.config
    }

    // ----- lifecycle steps -------------------------------------------

    pub fn initdb(&self) -> Result<()> {
        if let Some(parent) = self.config.data_dir.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::create_dir_all(&self.config.socket_dir)?;
        self.run(
            "initdb",
            [
                "-D",
                self.config.data_str(),
                "-U",
                "postgres",
                "--auth=trust",
                "--encoding=UTF8",
                "--locale=C",
                "--no-instructions",
            ],
        )?;
        Ok(())
    }

    /// Append walshadow's base settings to `postgresql.conf`. Safe to
    /// call multiple times; each invocation appends a fresh block.
    pub fn write_base_conf(&self) -> Result<()> {
        let conf_path = self.config.data_dir.join("postgresql.conf");
        let body = format!(
            "\n# walshadow base config\n\
             hot_standby = on\n\
             wal_level = replica\n\
             max_wal_senders = 0\n\
             autovacuum = off\n\
             fsync = off\n\
             full_page_writes = on\n\
             listen_addresses = ''\n\
             unix_socket_directories = '{sock}'\n\
             port = {port}\n\
             shared_buffers = 32MB\n",
            sock = self.config.socket_str(),
            port = self.config.port,
        );
        let mut f = fs::OpenOptions::new().append(true).open(&conf_path)?;
        f.write_all(body.as_bytes())?;
        Ok(())
    }

    /// Drop `standby.signal` + append `primary_conninfo` (walsender
    /// hot path) and `restore_command` (archive fallback) to
    /// `postgresql.conf`. PG's walreceiver tries `primary_conninfo`
    /// first and falls back to `restore_command` on connect error or
    /// end-of-WAL.
    pub fn enable_standby_recovery(&self, primary_conninfo: &str) -> Result<()> {
        let signal = self.config.data_dir.join("standby.signal");
        fs::write(&signal, b"")?;
        let conf_path = self.config.data_dir.join("postgresql.conf");
        let filter_dir = self
            .config
            .filter_out_dir
            .to_str()
            .expect("non-utf8 filter_out_dir");
        // Escape any embedded single-quote per PG conf-string
        // doubling convention.
        let escaped = primary_conninfo.replace('\'', "''");
        let body = format!(
            "\n# walshadow recovery\n\
             primary_conninfo = '{escaped}'\n\
             restore_command = 'cp {filter_dir}/%f %p'\n\
             recovery_target_timeline = 'latest'\n",
        );
        let mut f = fs::OpenOptions::new().append(true).open(&conf_path)?;
        f.write_all(body.as_bytes())?;
        Ok(())
    }

    pub fn start(&self) -> Result<()> {
        let log = self.config.data_dir.join("startup.log");
        self.run(
            "pg_ctl",
            [
                "-D",
                self.config.data_str(),
                "-l",
                log.to_str().expect("non-utf8 log path"),
                "-w",
                "-t",
                &self.config.ctl_timeout.as_secs().to_string(),
                "start",
            ],
        )?;
        Ok(())
    }

    pub fn stop(&self) -> Result<()> {
        self.run(
            "pg_ctl",
            [
                "-D",
                self.config.data_str(),
                "-m",
                "fast",
                "-w",
                "-t",
                &self.config.ctl_timeout.as_secs().to_string(),
                "stop",
            ],
        )?;
        Ok(())
    }

    pub fn is_running(&self) -> Result<bool> {
        let out = Command::new(self.config.bin("pg_ctl"))
            .args(["-D", self.config.data_str(), "status"])
            .output()?;
        // pg_ctl status exits 0 if running, 3 if not, 4 if no data dir
        Ok(out.status.code() == Some(0))
    }

    /// Optional oracle hook: load the `walshadow` extension if
    /// it's installed system-wide on shadow's PG. Tolerates the absent
    /// case — the daemon falls back to raw on-disk bytes for Tier 3
    /// types that aren't in the local matrix. Returns `true` iff the
    /// extension is now present.
    ///
    /// Operators install once via `(cd pgext && sudo make install)`;
    /// this method is a thin wrapper around the idempotent
    /// `CREATE EXTENSION IF NOT EXISTS walshadow`.
    pub fn try_load_oracle_extension(&self) -> Result<bool> {
        // `IF NOT EXISTS` keeps repeat calls a no-op; absence raises
        // `extension "walshadow" is not available`, which we
        // catch and surface as a clean "false" rather than failing the
        // bootstrap.
        match self.psql_one("CREATE EXTENSION IF NOT EXISTS walshadow") {
            Ok(_) => Ok(true),
            Err(ShadowError::Process { stderr, .. }) if stderr.contains("not available") => {
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }

    /// Feed a SQL payload to `psql -f -`. Used to apply
    /// `pg_dump --schema-only` output during bootstrap.
    pub fn apply_schema_dump(&self, sql: &str) -> Result<()> {
        let mut child = Command::new(self.config.bin("psql"))
            .args([
                "-h",
                self.config.socket_str(),
                "-p",
                &self.config.port.to_string(),
                "-U",
                "postgres",
                "-d",
                "postgres",
                "-v",
                "ON_ERROR_STOP=1",
                "-f",
                "-",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        child
            .stdin
            .as_mut()
            .expect("piped")
            .write_all(sql.as_bytes())?;
        let out = child.wait_with_output()?;
        self.check("psql -f -", out).map(|_| ())
    }

    // ----- probes ----------------------------------------------------

    /// Run a single SQL statement, return stdout trimmed. Uses
    /// `-tAXq` so each row is unadorned.
    pub fn psql_one(&self, sql: &str) -> Result<String> {
        let out = Command::new(self.config.bin("psql"))
            .args([
                "-h",
                self.config.socket_str(),
                "-p",
                &self.config.port.to_string(),
                "-U",
                "postgres",
                "-d",
                "postgres",
                "-tAXq",
                "-c",
                sql,
            ])
            .output()?;
        let out = self.check("psql -c", out)?;
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    pub fn is_in_recovery(&self) -> Result<bool> {
        match self.psql_one("SELECT pg_is_in_recovery()")?.as_str() {
            "t" => Ok(true),
            "f" => Ok(false),
            other => Err(ShadowError::PsqlParse(format!(
                "pg_is_in_recovery: got {other:?}, want t/f"
            ))),
        }
    }

    /// `pg_last_wal_replay_lsn()`. `None` when the function returns
    /// `NULL` (normal-mode cluster, or standby that has not yet
    /// replayed anything).
    pub fn last_replay_lsn(&self) -> Result<Option<u64>> {
        let s = self.psql_one("SELECT pg_last_wal_replay_lsn()")?;
        if s.is_empty() {
            return Ok(None);
        }
        Ok(Some(parse_pg_lsn(&s)?))
    }

    /// Block until shadow is in recovery and replay LSN ≥ `target`,
    /// or until `timeout` elapses. Returns the replay LSN observed.
    pub fn wait_for_replay(&self, target: u64, timeout: Duration) -> Result<u64> {
        let start = Instant::now();
        loop {
            if !self.is_in_recovery()? {
                return Err(ShadowError::NotInRecovery);
            }
            if let Some(lsn) = self.last_replay_lsn()?
                && lsn >= target
            {
                return Ok(lsn);
            }
            let elapsed = start.elapsed();
            if elapsed >= timeout {
                return Err(ShadowError::Timeout {
                    what: format!("pg_last_wal_replay_lsn >= {:#X}", target),
                    elapsed,
                });
            }
            thread::sleep(self.config.wait_poll);
        }
    }

    pub fn health(&self) -> Result<HealthReport> {
        let in_recovery = self.is_in_recovery()?;
        let replay_lsn = self.last_replay_lsn()?;
        let count_s = self.psql_one("SELECT count(*) FROM pg_class")?;
        let pg_class_count = count_s
            .parse::<u64>()
            .map_err(|e| ShadowError::PsqlParse(format!("count(*) FROM pg_class: {e}")))?;
        let pg_proc_relname =
            self.psql_one("SELECT relname FROM pg_class WHERE oid = 'pg_proc'::regclass")?;
        Ok(HealthReport {
            in_recovery,
            replay_lsn,
            pg_class_count,
            pg_proc_relname,
        })
    }

    // ----- internal --------------------------------------------------

    fn run<I, S>(&self, cmd: &str, args: I) -> Result<Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let bin = self.config.bin(cmd);
        let out = Command::new(&bin).args(args).output().map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                ShadowError::MissingBinary(cmd.into())
            } else {
                ShadowError::Io(e)
            }
        })?;
        self.check(cmd, out)
    }

    fn check(&self, cmd: &str, out: Output) -> Result<Output> {
        if out.status.success() {
            return Ok(out);
        }
        Err(ShadowError::Process {
            cmd: cmd.into(),
            status: out.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        })
    }
}

/// Parse PG's `pg_lsn` text form (`"XXXXXXXX/YYYYYYYY"`, hex) into a
/// 64-bit byte offset. Returns the same value `pg_lsn::bigint` would
/// in SQL.
pub fn parse_pg_lsn(s: &str) -> Result<u64> {
    let s = s.trim();
    let (hi, lo) = s
        .split_once('/')
        .ok_or_else(|| ShadowError::PsqlParse(format!("bad pg_lsn {s:?}: no '/'")))?;
    let hi = u32::from_str_radix(hi, 16)
        .map_err(|e| ShadowError::PsqlParse(format!("pg_lsn hi {hi:?}: {e}")))?;
    let lo = u32::from_str_radix(lo, 16)
        .map_err(|e| ShadowError::PsqlParse(format!("pg_lsn lo {lo:?}: {e}")))?;
    Ok(((hi as u64) << 32) | (lo as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_pg_lsn_basic() {
        assert_eq!(parse_pg_lsn("0/0").unwrap(), 0);
        assert_eq!(parse_pg_lsn("0/1").unwrap(), 1);
        assert_eq!(parse_pg_lsn("0/16B3750").unwrap(), 0x016B3750);
        assert_eq!(parse_pg_lsn("1/0").unwrap(), 1u64 << 32);
        assert_eq!(parse_pg_lsn("FFFFFFFF/FFFFFFFF").unwrap(), u64::MAX);
    }

    #[test]
    fn parse_pg_lsn_rejects() {
        assert!(parse_pg_lsn("").is_err());
        assert!(parse_pg_lsn("nope").is_err());
        assert!(parse_pg_lsn("0").is_err());
        assert!(parse_pg_lsn("0/Z").is_err());
    }

    #[test]
    fn config_socket_dir_default_sits_next_to_data_dir() {
        let cfg = ShadowConfig::new(
            PathBuf::from("/tmp/walshadow-test/data"),
            PathBuf::from("/tmp/walshadow-test/filtered"),
        );
        assert_eq!(
            cfg.socket_dir,
            PathBuf::from("/tmp/walshadow-test/shadow_sock")
        );
    }

    #[test]
    fn enable_standby_recovery_writes_signal_and_conf() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        let filter_dir = tmp.path().join("filtered");
        fs::create_dir_all(&data_dir).unwrap();
        fs::create_dir_all(&filter_dir).unwrap();
        // Empty postgresql.conf — append() requires the file to exist.
        fs::write(data_dir.join("postgresql.conf"), b"# base\n").unwrap();
        let cfg = ShadowConfig::new(data_dir.clone(), filter_dir);
        let shadow = Shadow::new(cfg);
        shadow
            .enable_standby_recovery(
                "host=/tmp/sock port=55555 user=walshadow application_name=shadow",
            )
            .unwrap();
        let conf = fs::read_to_string(data_dir.join("postgresql.conf")).unwrap();
        assert!(
            conf.contains("primary_conninfo"),
            "no conninfo line: {conf}"
        );
        assert!(
            conf.contains("restore_command"),
            "no restore_command: {conf}"
        );
        assert!(conf.contains("recovery_target_timeline"));
        assert!(data_dir.join("standby.signal").exists());
    }
}
