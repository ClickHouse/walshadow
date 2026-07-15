//! Shadow Postgres lifecycle: the co-located instance walshadow uses
//! as catalog mirror & decode oracle. Wraps PG binaries (`initdb`,
//! `pg_ctl`, `psql`) + on-disk plumbing (`postgresql.conf`,
//! `standby.signal`, `restore_command`).
//!
//! Bootstrap: [`initdb`](Shadow::initdb),
//! [`write_base_conf`](Shadow::write_base_conf),
//! [`start`](Shadow::start), [`apply_schema_dump`](Shadow::apply_schema_dump),
//! [`stop`](Shadow::stop),
//! [`enable_standby_recovery`](Shadow::enable_standby_recovery),
//! [`start`](Shadow::start), [`wait_for_replay`](Shadow::wait_for_replay),
//! [`health`](Shadow::health).
//!
//! `standby.signal` not `recovery.signal`: shadow is a *standby*.
//! `recovery.signal` exits recovery when archive WAL runs out; wrong
//! primitive. `standby.signal` stays in continuous recovery, retrying
//! `restore_command` on each new filter-landed segment.

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
    /// `-D` to all binaries.
    pub data_dir: PathBuf,
    /// `port`. Connections still go over the unix socket;
    /// `listen_addresses` stays empty.
    pub port: u16,
    pub socket_dir: PathBuf,
    /// Source path for shadow's `restore_command`.
    pub filter_out_dir: PathBuf,
    /// `None` resolves binaries via `$PATH`.
    pub pg_bin_dir: Option<PathBuf>,
    /// `pg_ctl -t`, both start and stop.
    pub ctl_timeout: Duration,
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

/// One-shot recovery & catalog state snapshot.
#[derive(Debug, Clone)]
pub struct HealthReport {
    pub in_recovery: bool,
    /// `pg_last_wal_replay_lsn()`; `None` before any WAL replayed
    /// (normal-mode startup or just-promoted standby).
    pub replay_lsn: Option<u64>,
    pub pg_class_count: u64,
    /// `relname` of the `pg_proc`-oid `pg_class` row. Always
    /// `"pg_proc"` healthy; surfaces catalog corruption or replay
    /// pinned far behind in one probe.
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

    /// Append walshadow base settings to `postgresql.conf`. Each call
    /// appends a fresh block.
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

    /// Drop `standby.signal` + append `primary_conninfo` and
    /// `restore_command`. PG's walreceiver tries `primary_conninfo`
    /// first, falls back to `restore_command` on connect error or
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
        // PG conf-string single-quote doubling
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
        // pg_ctl status: 0 running, 3 stopped, 4 no data dir
        Ok(out.status.code() == Some(0))
    }

    /// Load the `walshadow` extension if installed system-wide
    /// (operators run `(cd pgext && sudo make install)`). `true` iff
    /// now present; absence is tolerated, daemon falls back to raw
    /// on-disk bytes for Tier 3 types outside the local matrix.
    pub fn try_load_oracle_extension(&self) -> Result<bool> {
        // Absence raises `extension "walshadow" is not available`;
        // surface as clean false rather than failing bootstrap.
        match self.psql_one("CREATE EXTENSION IF NOT EXISTS walshadow") {
            Ok(_) => Ok(true),
            Err(ShadowError::Process { stderr, .. }) if stderr.contains("not available") => {
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }

    /// Feed a SQL payload to `psql -f -` (eg `pg_dump --schema-only`).
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

    /// Trimmed stdout of one statement. `-tAXq` keeps each row
    /// unadorned.
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

    /// `pg_last_wal_replay_lsn()`; `None` on `NULL` (normal-mode
    /// cluster, or standby that has not replayed anything).
    pub fn last_replay_lsn(&self) -> Result<Option<u64>> {
        let s = self.psql_one("SELECT pg_last_wal_replay_lsn()")?;
        if s.is_empty() {
            return Ok(None);
        }
        crate::pg::parse_pg_lsn(&s)
            .map(Some)
            .map_err(|e| ShadowError::PsqlParse(e.to_string()))
    }

    /// Block until in recovery and replay LSN ≥ `target`, or `timeout`.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_pg_lsn_basic() {
        assert_eq!(crate::pg::parse_pg_lsn("0/0").unwrap(), 0);
        assert_eq!(crate::pg::parse_pg_lsn("0/1").unwrap(), 1);
        assert_eq!(crate::pg::parse_pg_lsn("0/16B3750").unwrap(), 0x016B3750);
        assert_eq!(crate::pg::parse_pg_lsn("1/0").unwrap(), 1u64 << 32);
        assert_eq!(
            crate::pg::parse_pg_lsn("FFFFFFFF/FFFFFFFF").unwrap(),
            u64::MAX
        );
    }

    #[test]
    fn parse_pg_lsn_rejects() {
        assert!(crate::pg::parse_pg_lsn("").is_err());
        assert!(crate::pg::parse_pg_lsn("nope").is_err());
        assert!(crate::pg::parse_pg_lsn("0").is_err());
        assert!(crate::pg::parse_pg_lsn("0/Z").is_err());
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
        // append() requires the file to exist
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
