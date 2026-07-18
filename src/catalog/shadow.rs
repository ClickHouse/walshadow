//! Manage co-located Postgres used as catalog mirror and decode oracle
//! Run PG binaries (`initdb`, `pg_ctl`, `psql`) and write config files
//! (`postgresql.conf`, `standby.signal`, `restore_command`)
//!
//! Daemon path: [`control_guc_floor`](Shadow::control_guc_floor),
//! [`materialize_conf`](Shadow::materialize_conf),
//! [`write_standby_signal`](Shadow::write_standby_signal),
//! [`clear_stale_pid`](Shadow::clear_stale_pid),
//! [`start_with_floor_retry`](Shadow::start_with_floor_retry),
//! [`wait_for_replay`](Shadow::wait_for_replay),
//! [`is_running`](Shadow::is_running),
//! [`try_pg_wal_replay_resume`](Shadow::try_pg_wal_replay_resume),
//! [`health`](Shadow::health),
//! [`stop`](Shadow::stop). Test harness: [`initdb`](Shadow::initdb),
//! [`write_base_conf`](Shadow::write_base_conf),
//! [`apply_schema_dump`](Shadow::apply_schema_dump),
//! [`enable_standby_recovery`](Shadow::enable_standby_recovery).
//!
//! Use `standby.signal`, not `recovery.signal`. Recovery signal ends
//! recovery when archive runs out. Standby signal keeps recovery active
//! and retries `primary_conninfo`, then `restore_command`

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
    #[error("pg_controldata parse: {0}")]
    ControlDataParse(String),
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
    /// `-U` for `initdb` and all probe connections. Must match a role
    /// that exists post-seed, ie source's superuser role name on
    /// managed Postgres where it isn't literally `postgres`.
    pub user: String,
    /// `-d` for all probe connections.
    pub dbname: String,
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
            user: "postgres".to_string(),
            dbname: "postgres".to_string(),
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

/// Minimum standby GUC values
/// PG `CheckRequiredParameterValues` refuses hot standby startup when
/// any value is below primary value stored in `pg_control`
/// Read values from shadow using [`control_guc_floor`](Shadow::control_guc_floor)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceGucFloor {
    pub max_connections: u32,
    pub max_worker_processes: u32,
    pub max_wal_senders: u32,
    pub max_prepared_transactions: u32,
    pub max_locks_per_transaction: u32,
}

impl Default for SourceGucFloor {
    /// PG defaults for initdb cluster without source requirements
    fn default() -> Self {
        Self {
            max_connections: 100,
            max_worker_processes: 8,
            max_wal_senders: 10,
            max_prepared_transactions: 0,
            max_locks_per_transaction: 64,
        }
    }
}

impl SourceGucFloor {
    /// True when any field requires more than `running` provides, ie the
    /// state that pauses hot standby after `XLOG_PARAMETER_CHANGE`
    fn exceeds(&self, running: &SourceGucFloor) -> bool {
        self.max_connections > running.max_connections
            || self.max_worker_processes > running.max_worker_processes
            || self.max_wal_senders > running.max_wal_senders
            || self.max_prepared_transactions > running.max_prepared_transactions
            || self.max_locks_per_transaction > running.max_locks_per_transaction
    }
}

/// Outcome of [`try_pg_wal_replay_resume`](Shadow::try_pg_wal_replay_resume)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeOutcome {
    /// Replay running normally
    NotPaused,
    /// Paused by raised GUC floor; resumed to force shutdown then restart
    ResumedForFloor,
    /// Paused for another reason (operator `pg_wal_replay_pause`, recovery
    /// target); left untouched so the pause holds
    PausedForeign,
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
                self.config.user.as_str(),
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
             fsync = on\n\
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

    /// Replace data dir config with walshadow settings
    /// Write base, recovery, and minimum GUC settings to `postgresql.conf`
    /// Empty `postgresql.auto.conf` so source `ALTER SYSTEM` settings from
    /// BASE_BACKUP cannot override them. Allow trusted socket connections
    /// in `pg_hba.conf` and empty `pg_ident.conf`
    /// Do not depend on backup config because Debian stores it outside data
    /// dir under `/etc/postgresql/<v>/<cluster>`
    pub fn materialize_conf(
        &self,
        floor: &SourceGucFloor,
        primary_conninfo: Option<&str>,
    ) -> Result<()> {
        let filter_dir = self
            .config
            .filter_out_dir
            .to_str()
            .expect("non-utf8 filter_out_dir");
        let mut conf = format!(
            "# walshadow-owned conf, regenerated each boot\n\
             hot_standby = on\n\
             wal_level = replica\n\
             autovacuum = off\n\
             fsync = on\n\
             full_page_writes = on\n\
             listen_addresses = ''\n\
             unix_socket_directories = '{sock}'\n\
             port = {port}\n\
             shared_buffers = 32MB\n\
             max_connections = {max_connections}\n\
             max_worker_processes = {max_worker_processes}\n\
             max_wal_senders = {max_wal_senders}\n\
             max_prepared_transactions = {max_prepared_transactions}\n\
             max_locks_per_transaction = {max_locks_per_transaction}\n\
             restore_command = 'cp {filter_dir}/%f %p'\n\
             recovery_target_timeline = 'latest'\n",
            sock = self.config.socket_str(),
            port = self.config.port,
            max_connections = floor.max_connections,
            max_worker_processes = floor.max_worker_processes,
            max_wal_senders = floor.max_wal_senders,
            max_prepared_transactions = floor.max_prepared_transactions,
            max_locks_per_transaction = floor.max_locks_per_transaction,
        );
        if let Some(conninfo) = primary_conninfo {
            // Escape single quotes in PostgreSQL config string
            let escaped = conninfo.replace('\'', "''");
            conf.push_str(&format!("primary_conninfo = '{escaped}'\n"));
        }
        let d = &self.config.data_dir;
        fs::write(d.join("postgresql.conf"), conf)?;
        fs::write(
            d.join("postgresql.auto.conf"),
            "# walshadow: emptied, all config lives in postgresql.conf\n",
        )?;
        fs::write(
            d.join("pg_hba.conf"),
            "# walshadow-owned hba: socket-only, no TCP (listen_addresses = '')\n\
             local all all trust\n\
             local replication all trust\n",
        )?;
        fs::write(d.join("pg_ident.conf"), "# walshadow: unused\n")?;
        Ok(())
    }

    /// Create empty `standby.signal`
    pub fn write_standby_signal(&self) -> Result<()> {
        fs::write(self.config.data_dir.join("standby.signal"), b"")?;
        Ok(())
    }

    /// Return whether data dir contains initialized cluster
    pub fn data_dir_initialized(&self) -> bool {
        self.config.data_dir.join("PG_VERSION").exists()
    }

    /// Remove `postmaster.pid` left by stopped postmaster
    /// Call only after [`is_running`](Self::is_running) returns false
    /// Return whether file was removed
    pub fn clear_stale_pid(&self) -> Result<bool> {
        let pid = self.config.data_dir.join("postmaster.pid");
        match fs::remove_file(&pid) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
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
        let res = self.run(
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
        );
        match res {
            Ok(_) => Ok(()),
            // pg_ctl only reports "could not start server"
            // Include log where Postgres reports required GUC value
            Err(ShadowError::Process {
                cmd,
                status,
                stderr,
            }) => Err(ShadowError::Process {
                cmd,
                status,
                stderr: format!("{stderr}\nstartup.log tail:\n{}", log_tail(&log)),
            }),
            Err(e) => Err(e),
        }
    }

    /// Read minimum standby GUC values from local `pg_control`
    /// Seed copies source file. Replayed `XLOG_PARAMETER_CHANGE` updates it
    /// before startup stops, so next attempt reads new values
    pub fn control_guc_floor(&self) -> Result<SourceGucFloor> {
        let out = Command::new(self.config.bin("pg_controldata"))
            .args(["-D", self.config.data_str()])
            // Keep labels stable for parser
            .env("LC_ALL", "C")
            .output()
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    ShadowError::MissingBinary("pg_controldata".into())
                } else {
                    ShadowError::Io(e)
                }
            })?;
        let out = self.check("pg_controldata", out)?;
        parse_controldata_floor(&String::from_utf8_lossy(&out.stdout))
    }

    /// Write config with values from `pg_control` and start shadow
    /// If failed start updates required values, rewrite config and retry
    /// Return original error when values did not change
    pub fn start_with_floor_retry(&self, primary_conninfo: Option<&str>) -> Result<()> {
        let mut floor = self.control_guc_floor()?;
        loop {
            self.materialize_conf(&floor, primary_conninfo)?;
            let err = match self.start() {
                Ok(()) => return Ok(()),
                Err(e) => e,
            };
            let raised = self.control_guc_floor()?;
            if raised == floor {
                return Err(err);
            }
            self.clear_stale_pid()?;
            floor = raised;
        }
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
                self.config.user.as_str(),
                "-d",
                self.config.dbname.as_str(),
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
                self.config.user.as_str(),
                "-d",
                self.config.dbname.as_str(),
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

    /// Resume only a GUC-floor pause; leave other pauses intact
    /// Low hot standby GUC pauses replay after `XLOG_PARAMETER_CHANGE`,
    /// which writes the raised values to `pg_control` before pausing, so
    /// `control_guc_floor` exceeding the running settings identifies that
    /// cause. Resume shuts server down, then next
    /// [`start_with_floor_retry`](Self::start_with_floor_retry) reads new
    /// value from `pg_control`. An operator `pg_wal_replay_pause` (or a
    /// recovery-target pause) leaves floor equal to running, so it holds
    pub fn try_pg_wal_replay_resume(&self) -> Result<ResumeOutcome> {
        if self.psql_one("SELECT pg_get_wal_replay_pause_state()")? == "not paused" {
            return Ok(ResumeOutcome::NotPaused);
        }
        if !self
            .control_guc_floor()?
            .exceeds(&self.running_guc_floor()?)
        {
            return Ok(ResumeOutcome::PausedForeign);
        }
        self.psql_one("SELECT pg_wal_replay_resume()")?;
        Ok(ResumeOutcome::ResumedForFloor)
    }

    /// Read running GUC values with `current_setting`. Compare to
    /// [`control_guc_floor`](Self::control_guc_floor) to tell a floor-raise
    /// pause apart from other replay pauses
    fn running_guc_floor(&self) -> Result<SourceGucFloor> {
        let row = self.psql_one(
            "SELECT current_setting('max_connections'), \
             current_setting('max_worker_processes'), \
             current_setting('max_wal_senders'), \
             current_setting('max_prepared_transactions'), \
             current_setting('max_locks_per_transaction')",
        )?;
        let mut cols = row.split('|');
        let mut next = |name: &str| -> Result<u32> {
            cols.next()
                .ok_or_else(|| ShadowError::PsqlParse(format!("missing {name}")))
                .and_then(|v| {
                    v.parse()
                        .map_err(|_| ShadowError::PsqlParse(format!("{name} {v:?}")))
                })
        };
        Ok(SourceGucFloor {
            max_connections: next("max_connections")?,
            max_worker_processes: next("max_worker_processes")?,
            max_wal_senders: next("max_wal_senders")?,
            max_prepared_transactions: next("max_prepared_transactions")?,
            max_locks_per_transaction: next("max_locks_per_transaction")?,
        })
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

/// Parse required GUC values from `pg_controldata`
/// `LC_ALL=C` keeps labels stable; two labels use abbreviated `xact`
fn parse_controldata_floor(text: &str) -> Result<SourceGucFloor> {
    let find = |key: &str| -> Result<u32> {
        text.lines()
            .find_map(|line| line.strip_prefix(key))
            .ok_or_else(|| ShadowError::ControlDataParse(format!("missing {key:?}")))
            .and_then(|rest| {
                let v = rest.trim();
                v.parse()
                    .map_err(|_| ShadowError::ControlDataParse(format!("{key} {v:?}")))
            })
    };
    Ok(SourceGucFloor {
        max_connections: find("max_connections setting:")?,
        max_worker_processes: find("max_worker_processes setting:")?,
        max_wal_senders: find("max_wal_senders setting:")?,
        max_prepared_transactions: find("max_prepared_xacts setting:")?,
        max_locks_per_transaction: find("max_locks_per_xact setting:")?,
    })
}

/// Read end of log, where appended `pg_ctl -l` writes latest attempt
fn log_tail(path: &Path) -> String {
    use std::io::{Read, Seek, SeekFrom};
    const TAIL: u64 = 4096;
    let Ok(mut f) = fs::File::open(path) else {
        return "<unreadable>".into();
    };
    let len = f.metadata().map(|m| m.len()).unwrap_or(0);
    let _ = f.seek(SeekFrom::Start(len.saturating_sub(TAIL)));
    let mut buf = Vec::new();
    let _ = f.read_to_end(&mut buf);
    String::from_utf8_lossy(&buf).into_owned()
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
    fn parse_controldata_floor_reads_all_five() {
        let text = "pg_control version number:            1300\n\
                    max_connections setting:              500\n\
                    max_worker_processes setting:         16\n\
                    max_wal_senders setting:              12\n\
                    max_prepared_xacts setting:           2\n\
                    max_locks_per_xact setting:           128\n\
                    WAL block size:                       8192\n";
        let floor = parse_controldata_floor(text).unwrap();
        assert_eq!(
            floor,
            SourceGucFloor {
                max_connections: 500,
                max_worker_processes: 16,
                max_wal_senders: 12,
                max_prepared_transactions: 2,
                max_locks_per_transaction: 128,
            }
        );
    }

    #[test]
    fn parse_controldata_floor_rejects_missing_key() {
        let err = parse_controldata_floor("max_connections setting: 100\n").unwrap_err();
        assert!(matches!(err, ShadowError::ControlDataParse(_)), "{err}");
    }

    #[test]
    fn floor_exceeds_running_only_when_some_field_higher() {
        let running = SourceGucFloor::default();
        // Equal floor is an operator/other pause, not a raised requirement
        assert!(!running.exceeds(&running));
        // Any single raised field flags the parameter-change pause
        for raised in [
            SourceGucFloor {
                max_connections: running.max_connections + 1,
                ..running
            },
            SourceGucFloor {
                max_worker_processes: running.max_worker_processes + 1,
                ..running
            },
            SourceGucFloor {
                max_wal_senders: running.max_wal_senders + 1,
                ..running
            },
            SourceGucFloor {
                max_prepared_transactions: running.max_prepared_transactions + 1,
                ..running
            },
            SourceGucFloor {
                max_locks_per_transaction: running.max_locks_per_transaction + 1,
                ..running
            },
        ] {
            assert!(raised.exceeds(&running), "{raised:?}");
        }
        // Lower control floor than running never counts as exceeding
        let lower = SourceGucFloor {
            max_connections: running.max_connections - 1,
            ..running
        };
        assert!(!lower.exceeds(&running));
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
    fn materialize_conf_owns_every_conf_file() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).unwrap();
        // Replace source config copied by BASE_BACKUP
        fs::write(data_dir.join("postgresql.conf"), b"port = 5432\n").unwrap();
        fs::write(
            data_dir.join("postgresql.auto.conf"),
            b"listen_addresses = '*'\n",
        )
        .unwrap();
        let mut cfg = ShadowConfig::new(data_dir.clone(), tmp.path().join("filtered"));
        cfg.port = 5440;
        let shadow = Shadow::new(cfg);
        let floor = SourceGucFloor {
            max_connections: 500,
            ..SourceGucFloor::default()
        };
        shadow
            .materialize_conf(&floor, Some("host=127.0.0.1 port=5441 user=walshadow"))
            .unwrap();

        let conf = fs::read_to_string(data_dir.join("postgresql.conf")).unwrap();
        assert!(conf.contains("port = 5440"), "{conf}");
        assert!(
            !conf.contains("port = 5432"),
            "source config must be replaced: {conf}"
        );
        assert!(conf.contains("fsync = on"), "{conf}");
        assert!(conf.contains("max_connections = 500"), "{conf}");
        assert!(conf.contains("max_locks_per_transaction = 64"), "{conf}");
        assert!(conf.contains("restore_command"), "{conf}");
        assert!(conf.contains("recovery_target_timeline"), "{conf}");
        assert!(
            conf.contains("primary_conninfo = 'host=127.0.0.1"),
            "{conf}"
        );

        let auto = fs::read_to_string(data_dir.join("postgresql.auto.conf")).unwrap();
        assert!(
            !auto.contains("listen_addresses"),
            "auto.conf must be empty: {auto}"
        );
        let hba = fs::read_to_string(data_dir.join("pg_hba.conf")).unwrap();
        assert!(hba.contains("local replication all trust"), "{hba}");
        assert!(!hba.contains("host "), "socket-only: {hba}");
        assert!(data_dir.join("pg_ident.conf").exists());
    }

    #[test]
    fn materialize_conf_skips_conninfo_when_absent() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).unwrap();
        let cfg = ShadowConfig::new(data_dir.clone(), tmp.path().join("filtered"));
        let shadow = Shadow::new(cfg);
        shadow
            .materialize_conf(&SourceGucFloor::default(), None)
            .unwrap();
        let conf = fs::read_to_string(data_dir.join("postgresql.conf")).unwrap();
        assert!(!conf.contains("primary_conninfo"), "{conf}");
        assert!(conf.contains("restore_command"), "{conf}");
    }

    #[test]
    fn clear_stale_pid_reports_removal() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).unwrap();
        let shadow = Shadow::new(ShadowConfig::new(
            data_dir.clone(),
            tmp.path().join("filtered"),
        ));
        assert!(!shadow.data_dir_initialized());
        assert!(!shadow.clear_stale_pid().unwrap());
        fs::write(data_dir.join("postmaster.pid"), b"1234\n").unwrap();
        assert!(shadow.clear_stale_pid().unwrap());
        assert!(!data_dir.join("postmaster.pid").exists());
        fs::write(data_dir.join("PG_VERSION"), b"17\n").unwrap();
        assert!(shadow.data_dir_initialized());
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
