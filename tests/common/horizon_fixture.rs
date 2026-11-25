//! Shadow-TOAST horizon pin drills: one toasted table on source, history
//! target in ClickHouse, daemon on a physical slot

#![allow(dead_code)]

#[path = "bootstrap_ch_fixture.rs"]
pub mod fx;

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use walshadow::shadow::Shadow;

pub const SCHEMA: &str = "hz";
pub const ROWS: u32 = 400;
pub const BODY_REPEAT: u32 = 700;
pub const SLOT: &str = "walshadow_horizon";

pub fn write_config(path: &Path, ch_port: u16, pin: bool) -> Result<()> {
    let body = format!(
        "[ch]\n\
         host = \"127.0.0.1\"\n\
         port = {ch_port}\n\
         database = \"default\"\n\
         \n\
         [stream]\n\
         replicate_all = false\n\
         \n\
         [toast]\n\
         mode = \"shadow\"\n\
         read_ahead = false\n\
         source_horizon_pin = {pin}\n\
         \n\
         [table.\"{SCHEMA}\".\"t\"]\n\
         target_database = \"default\"\n\
         target_table = \"t\"\n\
         columns = [\n  \
           {{ attnum = 1, target = \"id\",   type = \"Int32\"  }},\n  \
           {{ attnum = 2, target = \"meta\", type = \"String\" }},\n  \
           {{ attnum = 3, target = \"body\", type = \"String\" }},\n\
         ]\n",
    );
    fs::write(path, body).with_context(|| format!("write {}", path.display()))
}

/// Dead TOAST chunks VACUUM could not remove
pub fn toast_dead_tuples(source: &Shadow) -> Result<u64> {
    let n = source.psql_one(&format!(
        "SELECT n_dead_tup FROM pg_stat_all_tables WHERE relid = \
         (SELECT reltoastrelid FROM pg_class WHERE oid = '{SCHEMA}.t'::regclass)"
    ))?;
    Ok(n.trim().parse()?)
}

pub fn slot_xmin(source: &Shadow) -> Result<Option<u64>> {
    let x = source.psql_one(&format!(
        "SELECT coalesce(xmin::text, '') FROM pg_replication_slots WHERE slot_name = '{SLOT}'"
    ))?;
    let x = x.trim();
    Ok((!x.is_empty()).then(|| x.parse()).transpose()?)
}

/// `(age(xmin), WAL bytes from restart_lsn to head)` of slot
pub fn slot_lag(source: &Shadow) -> Result<(u64, u64)> {
    let row = source.psql_one(&format!(
        "SELECT coalesce(age(xmin), 0)::text || ' ' || \
         pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint::text \
         FROM pg_replication_slots WHERE slot_name = '{SLOT}'"
    ))?;
    let (age, wal) = row.trim().split_once(' ').context("slot lag row")?;
    Ok((age.parse()?, wal.parse()?))
}

/// Source, ClickHouse and daemon layout. Drops source on exit
pub struct Rig {
    pub source: Shadow,
    pub ch: fx::ChServer,
    pub daemon: fx::DaemonRun,
    pub config: PathBuf,
    pub ports: fx::Ports,
    pub tmp: tempfile::TempDir,
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = self.source.stop();
    }
}

impl Rig {
    pub fn new(pin: bool) -> Option<Self> {
        if !fx::tools::requirements_available() {
            return None;
        }
        let ports = fx::Ports::alloc();
        let tmp = tempfile::tempdir().unwrap();
        let source = fx::start_source(&tmp);
        source
            .apply_schema_dump(&format!(
                "CREATE SCHEMA {SCHEMA};\n\
                 CREATE TABLE {SCHEMA}.t (id int4 PRIMARY KEY, meta text NOT NULL, \
                   body text NOT NULL) \
                   WITH (autovacuum_enabled = false, toast.autovacuum_enabled = false);\n\
                 ALTER TABLE {SCHEMA}.t ALTER COLUMN body SET STORAGE EXTERNAL;\n\
                 INSERT INTO {SCHEMA}.t SELECT g, 'v1', \
                   repeat('orig-'||g::text||'-', {BODY_REPEAT}) \
                   FROM generate_series(1, {ROWS}) g;\n\
                 CHECKPOINT;\n\
                 SELECT pg_switch_wal();\n"
            ))
            .expect("load source");
        let ch = fx::ChServer::spawn(tempfile::tempdir().unwrap(), ports.ch_tcp, ports.ch_http)
            .expect("spawn ch");
        // History table: every version stays, so a pointer-only row stays visible
        ch.query(
            "CREATE TABLE default.t (id Int32, meta String, body String, _lsn UInt64, \
             _xid UInt32, _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool) \
             ENGINE = MergeTree ORDER BY (id, _lsn)",
        )
        .expect("create ch table");
        let config = tmp.path().join("ch-config.toml");
        write_config(&config, ports.ch_tcp, pin).expect("write config");
        let daemon = fx::DaemonRun::prepare(tmp.path(), ports.metrics).expect("daemon layout");
        Some(Self {
            source,
            ch,
            daemon,
            config,
            ports,
            tmp,
        })
    }

    pub fn spawn(&self) -> Result<std::process::Child> {
        self.daemon.spawn(
            &self.source,
            &self.config,
            self.ports.walsender,
            &["--slot", SLOT],
        )
    }

    /// Wait for bootstrap rows, then move floor past them
    pub fn wait_bootstrapped(&self) -> Result<()> {
        fx::wait_for_listen(self.daemon.metrics_addr, Duration::from_secs(60))?;
        fx::wait_for_ch_value(
            &self.ch,
            "SELECT count() FROM default.t WHERE meta = 'v1'",
            &ROWS.to_string(),
            Duration::from_secs(60),
        )?;
        self.source.psql_one("SELECT pg_switch_wal()")?;
        self.source.psql_one("CHECKPOINT")?;
        std::thread::sleep(Duration::from_secs(3));
        Ok(())
    }

    pub fn superseded(&self) -> Result<u64> {
        let metrics = fx::http_get(self.daemon.metrics_addr, "/metrics")?;
        fx::parse_metric(&metrics, "walshadow_toast_values_filled_superseded_total")
            .context("superseded metric")
    }
}
