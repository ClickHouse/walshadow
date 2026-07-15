//! v1.0 acceptance §1: pgbench workload intermixed
//! with `ALTER TABLE ... ADD COLUMN ... DEFAULT k` (fast-path) and
//! `CREATE INDEX CONCURRENTLY`, end-state parity source vs CH.
//!
//! Pipeline mirrors `bootstrap_direct_ch.rs`:
//!
//! 1. source PG → `pgbench -i -s 1` (≈100k pgbench_accounts rows,
//!    1 branch, 10 tellers, empty history)
//! 2. `REPLICA IDENTITY FULL` on all four pgbench tables (preflight
//!    refuses non-FULL identity on tracked rels)
//! 3. spawn CH + pre-create dest tables ReplacingMergeTree(_lsn)
//! 4. spawn walshadow-stream `--bootstrap-mode=direct
//!    --bootstrap-shadow-data-dir` against four-table CH config
//! 5. await bootstrap (metrics endpoint up) → assert row counts match
//! 6. `pgbench -T 30 -c 4 -j 2` in background; at +10s ADD COLUMN c
//!    int DEFAULT 7 on pgbench_accounts (item 1 read-time defaults);
//!    at +20s CREATE INDEX CONCURRENTLY on pgbench_history (catalog-
//!    cache + non-blocking-DDL exercise)
//! 7. await pgbench exit; drain via `pg_switch_wal` + `--max-segments=1`
//! 8. parity oracle: count + sum + `c` column for accounts
//!
//! Skipped silently when `initdb`, `pg_basebackup`, `clickhouse`, or
//! `pgbench` aren't on `$PATH`. Linux-only — `Shadow` uses unix sockets.

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::fs;
use std::net::SocketAddr;
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result};
use walshadow::shadow::{Shadow, ShadowConfig};

// Port slots in the 17340 / 17360 ranges. Below the Linux ephemeral range
// so outbound connects can't grab a port we're about to bind. CH's
// `interserver_http_port = http_port + 1` must dodge metrics / walsender.
// Two disjoint sets so the 1/1 and 2/2 pool variants run concurrently.
#[derive(Clone, Copy)]
struct Ports {
    source: u16,
    shadow: u16,
    ch_tcp: u16,
    ch_http: u16,
    metrics: u16,
    walsender: u16,
}

const SERIAL_PORTS: Ports = Ports {
    source: 17341,
    shadow: 17342,
    ch_tcp: 17349,
    ch_http: 17350,
    metrics: 17355,
    walsender: 17356,
};

const POOLED_PORTS: Ports = Ports {
    source: 17361,
    shadow: 17362,
    ch_tcp: 17369,
    ch_http: 17370,
    metrics: 17375,
    walsender: 17376,
};

fn pgbench_available() -> bool {
    Command::new("pgbench")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn make_source(tmp: &tempfile::TempDir, port: u16) -> Shadow {
    let mut cfg = ShadowConfig::new(
        tmp.path().join("source-data"),
        tmp.path().join("source-filtered"),
    );
    cfg.port = port;
    cfg.socket_dir = tmp.path().join("source-sock");
    cfg.ctl_timeout = Duration::from_secs(60);
    fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    fs::create_dir_all(&cfg.socket_dir).unwrap();
    Shadow::new(cfg)
}

/// CH-config TOML covering all four pgbench tables. Attnums match
/// pgbench's `CREATE TABLE` order (see `pgbench --help` source, or
/// `\d pgbench_accounts` post-init).
///
/// `pgbench_accounts` advertises attnum 5 → target `c` even though
/// `c` doesn't exist in source's catalog at bootstrap time. Emitter
/// behaviour (per `ch_emitter::TableEncoder::append_row`): mapping
/// columns whose attnum isn't in the catalog descriptor land as NULL
/// on every row — CH dest must declare `c` Nullable so bootstrap
/// rows accept NULL. Post-ALTER UPDATE WAL records carry attnum=5 via
/// item 1's `attmissingval` substitution (decoder fills c=7), which
/// then arrives at CH as a non-NULL value and ReplacingMergeTree(_lsn)
/// promotes the post-ALTER copy over the bootstrap NULL.
fn write_pgbench_ch_config(
    path: &std::path::Path,
    ch_host: &str,
    ch_port: u16,
    ch_database: &str,
) -> Result<()> {
    let body = format!(
        "[ch]\n\
         host = \"{ch_host}\"\n\
         port = {ch_port}\n\
         database = \"{ch_database}\"\n\
         compression = \"lz4\"\n\
         \n\
         [table.public.pgbench_accounts]\n\
         columns = [\n  \
           {{ attnum = 1, target = \"aid\",      type = \"Int32\"  }},\n  \
           {{ attnum = 2, target = \"bid\",      type = \"Int32\"  }},\n  \
           {{ attnum = 3, target = \"abalance\", type = \"Int32\"  }},\n  \
           {{ attnum = 4, target = \"filler\",   type = \"String\" }},\n  \
           {{ attnum = 5, target = \"c\",        type = \"Nullable(Int32)\" }},\n\
         ]\n\
         \n\
         [table.public.pgbench_branches]\n\
         columns = [\n  \
           {{ attnum = 1, target = \"bid\",      type = \"Int32\"  }},\n  \
           {{ attnum = 2, target = \"bbalance\", type = \"Int32\"  }},\n  \
           {{ attnum = 3, target = \"filler\",   type = \"Nullable(String)\" }},\n\
         ]\n\
         \n\
         [table.public.pgbench_tellers]\n\
         columns = [\n  \
           {{ attnum = 1, target = \"tid\",      type = \"Int32\"  }},\n  \
           {{ attnum = 2, target = \"bid\",      type = \"Int32\"  }},\n  \
           {{ attnum = 3, target = \"tbalance\", type = \"Int32\"  }},\n  \
           {{ attnum = 4, target = \"filler\",   type = \"Nullable(String)\" }},\n\
         ]\n\
         \n\
         [table.public.pgbench_history]\n\
         columns = [\n  \
           {{ attnum = 1, target = \"tid\",    type = \"Int32\"  }},\n  \
           {{ attnum = 2, target = \"bid\",    type = \"Int32\"  }},\n  \
           {{ attnum = 3, target = \"aid\",    type = \"Int32\"  }},\n  \
           {{ attnum = 4, target = \"delta\",  type = \"Int32\"  }},\n  \
           {{ attnum = 5, target = \"mtime\",  type = \"DateTime64(6)\" }},\n  \
           {{ attnum = 6, target = \"filler\", type = \"Nullable(String)\" }},\n\
         ]\n",
    );
    fs::write(path, body).with_context(|| format!("write ch-config {}", path.display()))?;
    Ok(())
}

/// Pre-create the four pgbench destination tables on CH. Shape mirrors
/// pgbench's `CREATE TABLE` per `src/bin/pgbench/pgbench.c` plus the
/// synthetic `_lsn`/`_xid`/`_commit_ts`/`_is_deleted` trailer the emitter writes.
/// `pgbench_accounts` carries an extra `c Int32` for item 1's ADD COLUMN
/// surface.
fn create_pgbench_ch_tables(ch: &fx::ChServer, database: &str) -> Result<()> {
    ch.query(&format!("CREATE DATABASE IF NOT EXISTS {database}"))?;
    // `c Nullable(Int32)`: bootstrap rows arrive with c=NULL (source
    // catalog has no attnum=5 pre-ALTER); post-ALTER UPDATE WAL records
    // arrive with c=7 (item 1 attmissingval substitution).
    ch.query(&format!(
        "CREATE OR REPLACE TABLE {database}.pgbench_accounts (\
            aid Int32, bid Int32, abalance Int32, filler String, c Nullable(Int32),\
            _lsn UInt64, _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY aid"
    ))?;
    // filler on branches/tellers/history is char(NN) NULL by default
    // — pgbench's `-i` doesn't populate it (see pgbench.c "filler"
    // comment: only pgbench_accounts.filler gets blank-padded; the
    // other three default to NULL).
    ch.query(&format!(
        "CREATE OR REPLACE TABLE {database}.pgbench_branches (\
            bid Int32, bbalance Int32, filler Nullable(String),\
            _lsn UInt64, _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY bid"
    ))?;
    ch.query(&format!(
        "CREATE OR REPLACE TABLE {database}.pgbench_tellers (\
            tid Int32, bid Int32, tbalance Int32, filler Nullable(String),\
            _lsn UInt64, _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY tid"
    ))?;
    // pgbench_history has no PK; order by (tid, mtime, aid) for FINAL
    // merges, lean on `_lsn` for ReplacingMergeTree's version semantics.
    ch.query(&format!(
        "CREATE OR REPLACE TABLE {database}.pgbench_history (\
            tid Int32, bid Int32, aid Int32, delta Int32,\
            mtime DateTime64(6), filler Nullable(String),\
            _lsn UInt64, _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY (tid, mtime, aid)"
    ))?;
    Ok(())
}

/// Shell out to `psql` against the source PG. Used for mid-test DDL +
/// LSN probes that don't go through the `Shadow` wrapper. Returns
/// stderr-on-failure rather than panicking so the caller can attribute
/// the error.
fn psql_source(source: &Shadow, sql: &str) -> Result<String> {
    let out = Command::new("psql")
        .args([
            "-h",
            source.config().socket_dir.to_str().unwrap(),
            "-p",
            &source.config().port.to_string(),
            "-U",
            "postgres",
            "-d",
            "postgres",
            "-tAXq",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            sql,
        ])
        .output()
        .context("spawn psql")?;
    if !out.status.success() {
        anyhow::bail!(
            "psql failed: {sql}\nstderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// Run `pgbench -i -s 1` against source. Creates the four standard
/// pgbench tables + populates them.
fn pgbench_init(source: &Shadow, scale: u32) -> Result<()> {
    let out = Command::new("pgbench")
        .args([
            "-h",
            source.config().socket_dir.to_str().unwrap(),
            "-p",
            &source.config().port.to_string(),
            "-U",
            "postgres",
            "-d",
            "postgres",
            "-i",
            "-s",
            &scale.to_string(),
            // -q quieter init output
            "-q",
        ])
        .output()
        .context("spawn pgbench -i")?;
    if !out.status.success() {
        anyhow::bail!(
            "pgbench -i failed: stderr={}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(())
}

/// Parametrized body: `decoder_pool` / `inserter_pool` are the daemon's
/// `--decoder-pool-size` / `--inserter-pool-size`. At 2/2 this drives N
/// concurrent `AsyncClient`s for both bootstrap and WAL under the DDL
/// barrier, asserting out-of-order INSERTs across connections stay
/// `_lsn`-correct (the parity oracle at the end).
async fn run_ddl_intermix(ports: Ports, decoder_pool: usize, inserter_pool: usize, label: &str) {
    if !fx::pg_available() {
        tracing::warn!("skip: no initdb on PATH");
        return;
    }
    if !fx::pg_basebackup_available() {
        tracing::warn!("skip: no pg_basebackup on PATH");
        return;
    }
    if !fx::clickhouse_available() {
        tracing::warn!("skip: no clickhouse binary on PATH");
        return;
    }
    if !pgbench_available() {
        tracing::warn!("skip: no pgbench binary on PATH");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();

    // 1. Source PG.
    let source = make_source(&tmp, ports.source);
    source.initdb().expect("initdb source");
    source.write_base_conf().expect("source base conf");
    fx::append_source_conf(&source).expect("append source conf");
    source.start().expect("start source");
    let _src_stop = fx::StopOnDrop { sh: &source };

    // 2. pgbench -i -s 1: creates pgbench_{accounts,branches,tellers,
    //    history} + populates them.
    pgbench_init(&source, 1).expect("pgbench -i -s 1");

    // 3. REPLICA IDENTITY FULL on all four tables — preflight refuses
    //    tracked rels without FULL identity. pgbench_history has no PK
    //    so FULL is required regardless; the other three default to
    //    DEFAULT which surfaces UPDATE old-tuple without the unchanged
    //    columns, breaking CH's row identity.
    for t in [
        "pgbench_accounts",
        "pgbench_branches",
        "pgbench_tellers",
        "pgbench_history",
    ] {
        psql_source(&source, &format!("ALTER TABLE {t} REPLICA IDENTITY FULL"))
            .unwrap_or_else(|e| panic!("ALTER {t}: {e}"));
    }

    // 4. CH server + dest tables.
    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, ports.ch_tcp, ports.ch_http).expect("spawn ch");
    create_pgbench_ch_tables(&ch, "default").expect("create ch dest tables");

    // 5. CH-config TOML.
    let ch_config_path = tmp.path().join("ch-config.toml");
    write_pgbench_ch_config(&ch_config_path, "127.0.0.1", ports.ch_tcp, "default")
        .expect("write ch-config");

    // 6. Daemon-owned shadow layout, same quirk as direct-bootstrap drill.
    let bootstrap_shadow_data_dir = tmp.path().join("shadow-data");
    let shadow_sock = tmp.path().join("shadow-sock");
    fs::create_dir_all(&shadow_sock).unwrap();
    let shadow_filter_dir = tmp.path().join("filtered");
    fs::create_dir_all(&shadow_filter_dir).unwrap();
    let spill_dir = tmp.path().join("spill");
    fs::create_dir_all(&spill_dir).unwrap();

    // 7. Spawn walshadow-stream. `--max-segments=1` gates clean exit
    //    once we trigger `pg_switch_wal` post-workload.
    let bin = env!("CARGO_BIN_EXE_walshadow-stream");
    let stderr_path = tmp.path().join("daemon.stderr.log");
    let stderr_file = fs::File::create(&stderr_path).expect("open daemon stderr log");
    let metrics_addr: SocketAddr = format!("127.0.0.1:{}", ports.metrics).parse().unwrap();
    let decoder_pool_arg = decoder_pool.to_string();
    let inserter_pool_arg = inserter_pool.to_string();
    let child = Command::new(bin)
        .args([
            "--host",
            source.config().socket_dir.to_str().unwrap(),
            "--port",
            &source.config().port.to_string(),
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
            // Hold INSERTs open across xacts; pgbench TPC-B writes 4
            // tables/xact and each per-table close is one CH
            // EndOfStream round-trip, so flush_timeout=0 caps throughput
            // at ~one xact per (4 × RTT) = ~5 xact/s on a local CH.
            // 200 ms coalesces inserts into one MergeTree part per
            // window and lets the daemon track pgbench's ~700 xact/s.
            "--ch-flush-timeout-ms",
            "200",
            "--bootstrap-mode",
            "direct",
            "--bootstrap-shadow-data-dir",
            bootstrap_shadow_data_dir.to_str().unwrap(),
            "--bootstrap-shadow-replay-timeout",
            "180",
            // N>1 here drives concurrent AsyncClients for bootstrap +
            // WAL; the end-state parity oracle proves out-of-order
            // INSERTs across connections stay _lsn-correct.
            "--decoder-pool-size",
            &decoder_pool_arg,
            "--inserter-pool-size",
            &inserter_pool_arg,
        ])
        // CI sets `WALSHADOW_ARTIFACT_DIR`; bump the daemon to trace
        // for `xact_buffer` so a stalled commit pipeline can be read
        // straight off the artifact's `daemon.stderr.log`. Local runs
        // leave the env var unset and keep the quieter default.
        .env(
            "RUST_LOG",
            if std::env::var_os("WALSHADOW_ARTIFACT_DIR").is_some() {
                "warn,walshadow=info,walshadow::xact_buffer=trace"
            } else {
                "warn,walshadow=info"
            },
        )
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr_file))
        .process_group(0)
        .spawn()
        .expect("spawn walshadow-stream");
    let guard = fx::ChildGuard::new(child);

    let result = (|| -> Result<()> {
        // 8. Wait for the daemon's metrics endpoint (liveness). The daemon
        //    binds it before the ≈100k-row bootstrap drains to CH, so it is
        //    not a bootstrap-complete signal on its own.
        fx::wait_for_listen(metrics_addr, Duration::from_secs(300))
            .context("daemon metrics endpoint never came up")?;

        // 9. Post-bootstrap row-count parity. pgbench -i -s 1 produces
        //    deterministic counts: 100000 accounts, 1 branch, 10 tellers,
        //    0 history rows. The bootstrap drains asynchronously, so poll CH
        //    until it matches rather than racing an immediate assert.
        for (src_table, ch_table, expected) in [
            ("pgbench_accounts", "default.pgbench_accounts", 100_000_u64),
            ("pgbench_branches", "default.pgbench_branches", 1),
            ("pgbench_tellers", "default.pgbench_tellers", 10),
            ("pgbench_history", "default.pgbench_history", 0),
        ] {
            let src = psql_source(&source, &format!("SELECT count(*) FROM {src_table}"))?;
            anyhow::ensure!(
                src == expected.to_string(),
                "source {src_table} count {src} != expected {expected}"
            );
            let deadline = std::time::Instant::now() + Duration::from_secs(120);
            loop {
                let chc = ch
                    .query(&format!(
                        "SELECT count() FROM {ch_table} FINAL WHERE _is_deleted = 0"
                    ))
                    .unwrap_or_default();
                if chc == src {
                    break;
                }
                if std::time::Instant::now() >= deadline {
                    anyhow::bail!("bootstrap mismatch {src_table}: source={src}, ch={chc}");
                }
                std::thread::sleep(Duration::from_millis(300));
            }
        }

        // 10. Background pgbench workload. -T 6 wallclock keeps the
        //     test seconds-scale while still exercising thousands of
        //     UPDATEs against pgbench_accounts (~hundreds of distinct
        //     aids touched, so item 1's attmissingval surface is
        //     hit on real WAL traffic). -c 4 / -j 2 keeps CI load
        //     reasonable.
        let mut pgbench_child = Command::new("pgbench")
            .args([
                "-h",
                source.config().socket_dir.to_str().unwrap(),
                "-p",
                &source.config().port.to_string(),
                "-U",
                "postgres",
                "-d",
                "postgres",
                "-T",
                "6",
                "-c",
                "4",
                "-j",
                "2",
                "-n", // skip VACUUM (already vacuumed by -i)
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("spawn pgbench -T 6")?;

        // 11. +2s: ALTER TABLE pgbench_accounts ADD COLUMN c int
        //     DEFAULT 7 — fast-path ADD COLUMN that exercises item 1's
        //     read-time defaults. pre-ALTER WAL records carry natts=4
        //     and the decoder substitutes attmissingval[1]=7 for the
        //     new column.
        std::thread::sleep(Duration::from_secs(2));
        psql_source(
            &source,
            "ALTER TABLE pgbench_accounts ADD COLUMN c int DEFAULT 7",
        )
        .context("ADD COLUMN c")?;

        // 12. +2s after ALTER: CREATE INDEX CONCURRENTLY on
        //     pgbench_history(bid). Long-running catalog-update sequence
        //     under concurrent writers; exercises shadow-catalog cache
        //     invalidation.
        std::thread::sleep(Duration::from_secs(2));
        psql_source(
            &source,
            "CREATE INDEX CONCURRENTLY ON pgbench_history (bid)",
        )
        .context("CREATE INDEX CONCURRENTLY")?;

        // 13. Wait for pgbench to finish. -T 6 wallclock + startup; 30s
        //     budget covers slow CI.
        let pgbench_status = wait_child_with_timeout(&mut pgbench_child, Duration::from_secs(30))
            .context("pgbench -T 6 exit")?;
        anyhow::ensure!(
            pgbench_status.success(),
            "pgbench -T 6 exit status: {pgbench_status:?}"
        );

        // 14. Force a segment seal so the daemon's pump definitely
        //     reaches every committed row in WAL, then poll the daemon's
        //     `walshadow_emitter_ack_lsn` until it crosses source's
        //     post-switch `pg_current_wal_lsn`. ChildGuard's Drop will
        //     SIGKILL the daemon once assertions pass.
        psql_source(&source, "SELECT pg_switch_wal()").context("pg_switch_wal")?;
        let target_lsn_text =
            psql_source(&source, "SELECT pg_current_wal_lsn()::text").context("read source LSN")?;
        let target_lsn =
            walshadow::pg::parse_pg_lsn(&target_lsn_text).context("parse source LSN")?;
        wait_for_ack_catchup(metrics_addr, target_lsn, Duration::from_secs(60))
            .context("emitter ack catchup")?;

        // 15. Optional: nudge CH to merge so FINAL queries are cheap.
        //     ReplacingMergeTree(_lsn) collapses duplicate (aid)-keyed
        //     rows down to the highest _lsn.
        for t in [
            "pgbench_accounts",
            "pgbench_branches",
            "pgbench_tellers",
            "pgbench_history",
        ] {
            let _ = ch.query(&format!("OPTIMIZE TABLE default.{t} FINAL"));
        }

        // 16. Parity assertions per table.
        for (src_table, ch_table, sum_col) in [
            ("pgbench_accounts", "default.pgbench_accounts", "abalance"),
            ("pgbench_branches", "default.pgbench_branches", "bbalance"),
            ("pgbench_tellers", "default.pgbench_tellers", "tbalance"),
            ("pgbench_history", "default.pgbench_history", "delta"),
        ] {
            let src_count = psql_source(&source, &format!("SELECT count(*) FROM {src_table}"))?;
            let ch_count = ch.query(&format!(
                "SELECT count() FROM {ch_table} FINAL WHERE _is_deleted = 0"
            ))?;
            anyhow::ensure!(
                src_count == ch_count,
                "{src_table} count mismatch: source={src_count}, ch={ch_count}"
            );

            let src_sum = psql_source(
                &source,
                &format!("SELECT coalesce(sum({sum_col}), 0)::text FROM {src_table}"),
            )?;
            let ch_sum = ch.query(&format!(
                "SELECT sum({sum_col}) FROM {ch_table} FINAL WHERE _is_deleted = 0"
            ))?;
            // CH returns "0" for empty sum; PG via coalesce returns "0"
            // as well. pgbench_history starts empty pre-workload but
            // gathers one INSERT per pgbench transaction.
            anyhow::ensure!(
                src_sum == ch_sum,
                "{src_table} sum({sum_col}) mismatch: source={src_sum}, ch={ch_sum}"
            );
        }

        // 17. Item 1 surface: every non-NULL `c` value in CH must be 7.
        //     pgbench's TPC-B xact runs `update pgbench_accounts set
        //     abalance = ... where aid = :random` so post-ALTER UPDATE
        //     WAL records carry natts=4 (pre-ALTER tuple shape) and
        //     decoder substitutes attmissingval[c]=7 for the new column.
        //     ReplacingMergeTree(_lsn) promotes the post-ALTER copy over
        //     the bootstrap NULL for any row touched during the
        //     workload. Bootstrap-only rows (never UPDATEd by pgbench)
        //     stay at c=NULL — pgbench -T 30 -c 4 on 100k accounts won't
        //     touch every row.
        //
        //     Assertion: (a) at least one row has c=7 (proves item 1's
        //     substitution path fired end-to-end), (b) no row has c set
        //     to anything other than 7 or NULL.
        let updated_count = ch.query(
            "SELECT count() FROM default.pgbench_accounts FINAL \
             WHERE _is_deleted = 0 AND c IS NOT NULL",
        )?;
        anyhow::ensure!(
            updated_count.parse::<u64>().unwrap_or(0) > 0,
            "no pgbench_accounts row reached CH with non-NULL c — item 1 read-time \
             default substitution did not fire (updated_count={updated_count})"
        );
        let off_value = ch.query(
            "SELECT count() FROM default.pgbench_accounts FINAL \
             WHERE _is_deleted = 0 AND c IS NOT NULL AND c != 7",
        )?;
        anyhow::ensure!(
            off_value == "0",
            "pgbench_accounts.c has rows with c != 7 (and not NULL): count={off_value}"
        );

        Ok(())
    })();

    // Kill daemon before shadow so supervisor cannot restart it
    // Stop any remaining postmaster before tempdir cleanup
    let _ = guard.into_inner().map(|mut c| {
        let _ = c.kill();
        let _ = c.wait();
    });
    if bootstrap_shadow_data_dir.join("postmaster.pid").exists() {
        let mut shadow_cfg =
            ShadowConfig::new(bootstrap_shadow_data_dir.clone(), shadow_filter_dir.clone());
        shadow_cfg.port = ports.shadow;
        shadow_cfg.socket_dir = shadow_sock.clone();
        shadow_cfg.ctl_timeout = Duration::from_secs(60);
        let shadow = Shadow::new(shadow_cfg);
        let _ = shadow.stop();
    }

    if let Err(e) = result {
        let stderr = fs::read_to_string(&stderr_path).unwrap_or_default();
        // Snapshot the whole tempdir (shadow PG log, daemon stderr,
        // spill, filtered manifests) before TempDir::drop wipes it. CI
        // uploads $WALSHADOW_ARTIFACT_DIR as a job artifact; local runs
        // leave the env var unset and dump_artifacts no-ops.
        fx::dump_artifacts(tmp.path(), label);
        panic!("{e:#}\n--- daemon stderr ---\n{stderr}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgbench_acceptance_ddl_intermix() {
    run_ddl_intermix(SERIAL_PORTS, 1, 1, "pgbench_acceptance_ddl_intermix").await;
}

/// Same drill at decoder/inserter pool 2/2 — the live daemon coverage for
/// N>1 concurrent `AsyncClient`s under the DDL barrier that the in-process
/// `pipeline_parallel_{e2e,ddl_e2e}` tests can't provide.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgbench_acceptance_ddl_intermix_pooled() {
    run_ddl_intermix(POOLED_PORTS, 2, 2, "pgbench_acceptance_ddl_intermix_pooled").await;
}

/// Poll a `std::process::Child` until exit or timeout. Mirrors
/// `fx::wait_with_timeout` shape but doesn't kill on timeout — caller
/// owns post-timeout reaping for pgbench so test cleanup can see the
/// process state.
fn wait_child_with_timeout(
    child: &mut std::process::Child,
    deadline: Duration,
) -> Result<std::process::ExitStatus> {
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        match child.try_wait()? {
            Some(s) => return Ok(s),
            None => std::thread::sleep(Duration::from_millis(200)),
        }
    }
    let _ = child.kill();
    let _ = child.wait();
    anyhow::bail!("pgbench did not exit within {deadline:?}");
}

/// Poll the daemon's `/metrics` endpoint until
/// `walshadow_emitter_ack_lsn >= target`. Read: "the daemon has flushed
/// every row up through `target` to CH." Used as the test's drain gate
/// in place of `--max-segments` (which races with bootstrap-triggered
/// segment seals).
fn wait_for_ack_catchup(metrics_addr: SocketAddr, target: u64, deadline: Duration) -> Result<u64> {
    let start = std::time::Instant::now();
    let mut last_body = String::new();
    while start.elapsed() < deadline {
        if let Ok(body) = fx::http_get(metrics_addr, "/metrics") {
            last_body = body;
            if let Some(ack) = fx::parse_metric(&last_body, "walshadow_emitter_ack_lsn")
                && ack >= target
            {
                return Ok(ack);
            }
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    if let Ok(root) = std::env::var("WALSHADOW_ARTIFACT_DIR")
        && !last_body.is_empty()
    {
        let path = std::path::PathBuf::from(root).join("metrics.txt");
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = std::fs::write(&path, &last_body);
    }
    anyhow::bail!("emitter_ack_lsn never reached {target:X} in {deadline:?}");
}
