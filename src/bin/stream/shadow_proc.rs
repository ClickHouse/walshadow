//! Owned shadow PG: conf materialization, start, and supervised restart.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio_util::sync::CancellationToken;
use walrus::pg::backup::format_pg_lsn;
use walshadow::ch_emitter::EmitterConfig;
use walshadow::pg::socket_conninfo;
use walshadow::shadow::{ResumeOutcome, Shadow, ShadowConfig};

use crate::args::Args;

/// tokio_postgres client against shadow over its unix socket, for
/// [`walshadow::preflight::run`] which needs SQL access independent of
/// [`ShadowCatalog`](walshadow::shadow_catalog::ShadowCatalog)'s replay-LSN-gated path.
pub(crate) async fn open_shadow_sql_client(
    socket_dir: &std::path::Path,
    port: u16,
    user: &str,
    dbname: &str,
) -> Result<tokio_postgres::Client> {
    let socket = socket_dir.to_str().context("shadow-socket-dir not UTF-8")?;
    let conninfo = socket_conninfo(socket, port, user, dbname);
    let (client, conn) = tokio_postgres::connect(&conninfo, tokio_postgres::NoTls)
        .await
        .context(format!("preflight: open shadow sql client ({conninfo})"))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Ok(client)
}

pub(crate) fn bridge_pool_size(ch_config: Option<&EmitterConfig>) -> usize {
    ch_config
        .map_or(1, |cfg| cfg.inserter_pool_size)
        .clamp(1, walshadow::bridge::MAX_BRIDGE_WORKERS)
}

pub(crate) fn build_owned_shadow(
    args: &Args,
    dbname: &str,
    databases: &[String],
    data_dir: PathBuf,
    workers: usize,
) -> Shadow {
    let mut cfg = ShadowConfig::new(data_dir, args.out_dir.clone());
    cfg.port = args.shadow_port;
    cfg.socket_dir = args.shadow_socket_dir.clone();
    cfg.ctl_timeout = Duration::from_secs(args.shadow_connect_timeout);
    cfg.user = args.shadow_user.clone();
    cfg.dbname = dbname.to_string();
    let mut bridge = walshadow::shadow::BridgeConf::in_dir(&cfg.socket_dir);
    bridge.socket_path = args.bridge_socket_path();
    bridge.library_dir = args.bridge_lib_dir.clone();
    bridge.workers = workers;
    bridge.databases = databases.to_vec();
    cfg.bridge = Some(bridge);
    Shadow::new(cfg)
}

pub(crate) fn walsender_primary_conninfo(bind: SocketAddr) -> String {
    format!(
        "host={} port={} user=walshadow application_name=shadow sslmode=disable",
        bind.ip(),
        bind.port(),
    )
}

/// Start daemon-owned shadow using archived WAL
/// After fresh bootstrap, wait for backup `end_lsn`; direct mode includes
/// required WAL in `base.tar`
/// Restart a postmaster left alive by an unclean prior exit so it binds
/// this daemon's port and socket
pub(crate) async fn start_owned_shadow(
    shadow: &Arc<Shadow>,
    replay_target: Option<u64>,
    replay_timeout: Duration,
    keep_running: bool,
) -> Result<()> {
    let s = shadow.clone();
    tokio::task::spawn_blocking(move || -> Result<()> {
        // Prior daemon may have died mid `pg_ctl start -w`
        if keep_running
            && s.wait_started()
                .context("wait for running shadow startup")?
        {
            s.validate_running().context("validate running shadow")?;
            warn_database_absent(&s);
            tracing::info!(target: "walshadow::shadow", "reusing running shadow");
            return Ok(());
        }
        if s.is_running().context("shadow status probe")? {
            // Adopt only fires after unclean prior exit left the postmaster
            // alive holding stale port/socket/primary_conninfo. Stop so the
            // restart below binds params this daemon connects and streams with;
            // start_with_floor_retry regenerates conf.
            tracing::warn!(
                target: "walshadow::shadow",
                "shadow alive from unclean exit; restarting under fresh config",
            );
            s.stop().context("stop stale shadow before restart")?;
        }
        s.clear_stale_pid().context("clear stale postmaster.pid")?;
        s.start_with_floor_retry(None).context("shadow start")?;
        if let Some(target) = replay_target {
            let lsn = s
                .wait_for_replay(target, replay_timeout)
                .context("wait for shadow replay of bootstrap end_lsn")?;
            tracing::info!(
                target: "walshadow::shadow",
                replay_lsn = format_pg_lsn(lsn).to_string(),
                "shadow caught up to bootstrap end_lsn",
            );
        }
        warn_database_absent(&s);
        Ok(())
    })
    .await
    .context("shadow start task")?
}

/// Supervisor pacing: liveness probe period, and restart backoff doubling
/// from `restart` up to `restart_max`
#[derive(Clone, Copy)]
pub(crate) struct SupervisePace {
    pub(crate) probe: Duration,
    pub(crate) restart: Duration,
    pub(crate) restart_max: Duration,
}

pub(crate) const SUPERVISE_PACE: SupervisePace = SupervisePace {
    probe: Duration::from_secs(2),
    restart: Duration::from_secs(1),
    restart_max: Duration::from_secs(60),
};

/// Daemon-owned postmaster, stopped on drop unless `keep_running`. Wraps a
/// shadow before its start, so every exit path after it stops what started
pub(crate) struct OwnedShadow {
    pub(crate) shadow: Arc<Shadow>,
    pub(crate) keep_running: bool,
}

impl OwnedShadow {
    pub(crate) fn new(shadow: Shadow, keep_running: bool) -> Self {
        Self {
            shadow: Arc::new(shadow),
            keep_running,
        }
    }
}

impl Drop for OwnedShadow {
    fn drop(&mut self) {
        if self.keep_running {
            return;
        }
        // Daemon is exiting, blocking pg_ctl cannot delay other work
        match self.shadow.is_running() {
            Ok(true) => {
                if let Err(e) = self.shadow.stop() {
                    tracing::warn!(
                        target: "walshadow::shadow",
                        error = %e,
                        "shadow stop on daemon exit failed",
                    );
                }
            }
            Ok(false) => {}
            Err(e) => tracing::warn!(
                target: "walshadow::shadow",
                error = %e,
                "shadow status probe on daemon exit failed",
            ),
        }
    }
}

/// Supervise daemon-owned shadow, restarting stopped postmaster with
/// backoff. `ShadowCatalog` reconnects after restart
/// Read minimum GUC values from `pg_control` before each restart because
/// replayed `XLOG_PARAMETER_CHANGE` may raise them
/// Call `shutdown` on clean exit; Drop is just a fallback, its abort
/// can race a restart already in flight on the blocking pool
pub(crate) struct ShadowLifecycle {
    pub(crate) guard: OwnedShadow,
    pub(crate) supervisor: Option<tokio::task::JoinHandle<()>>,
    pub(crate) cancel: CancellationToken,
}

impl ShadowLifecycle {
    pub(crate) fn spawn(guard: OwnedShadow, conninfo: String) -> Self {
        let cancel = CancellationToken::new();
        let supervisor = tokio::spawn(Self::supervise(
            guard.shadow.clone(),
            conninfo,
            cancel.clone(),
            SUPERVISE_PACE,
        ));
        Self {
            guard,
            supervisor: Some(supervisor),
            cancel,
        }
    }

    pub(crate) async fn supervise(
        shadow: Arc<Shadow>,
        conninfo: String,
        cancel: CancellationToken,
        pace: SupervisePace,
    ) {
        let mut backoff = pace.restart;
        // Edge-trigger the foreign-pause log so a held operator pause does
        // not spam once per tick
        let mut foreign_logged = false;
        loop {
            tokio::select! {
                () = cancel.cancelled() => return,
                () = tokio::time::sleep(pace.probe) => {}
            }
            match probe_blocking(&shadow, |s| s.is_running()).await {
                Some(true) => {
                    backoff = pace.restart;
                    // Higher GUC requirement pauses active hot standby
                    // Resume forces shutdown, then restart uses new values
                    // Ignore probe errors while psql waits for consistency
                    let s = shadow.clone();
                    let outcome =
                        tokio::task::spawn_blocking(move || s.try_pg_wal_replay_resume()).await;
                    match outcome {
                        Ok(Ok(ResumeOutcome::ResumedForFloor)) => {
                            foreign_logged = false;
                            tracing::warn!(
                                target: "walshadow::shadow",
                                "shadow replay paused because GUC value is below primary; \
                                 resumed replay to restart with required value",
                            );
                        }
                        Ok(Ok(ResumeOutcome::PausedForeign)) => {
                            if !foreign_logged {
                                foreign_logged = true;
                                tracing::info!(
                                    target: "walshadow::shadow",
                                    "shadow replay paused for a reason other than GUC floor \
                                     (eg operator pg_wal_replay_pause); leaving paused",
                                );
                            }
                        }
                        Ok(Ok(ResumeOutcome::NotPaused)) => foreign_logged = false,
                        _ => {}
                    }
                }
                Some(false) => {
                    tracing::warn!(
                        target: "walshadow::shadow",
                        "shadow postmaster stopped, restarting",
                    );
                    let ci = conninfo.clone();
                    let restarted = probe_blocking(&shadow, move |s| {
                        s.clear_stale_pid()?;
                        s.start_with_floor_retry(Some(&ci))
                    })
                    .await;
                    if restarted.is_some() {
                        tracing::info!(target: "walshadow::shadow", "shadow restarted");
                        backoff = pace.restart;
                    } else {
                        tokio::select! {
                            () = cancel.cancelled() => return,
                            () = tokio::time::sleep(backoff) => {}
                        }
                        backoff = (backoff * 2).min(pace.restart_max);
                    }
                }
                None => {}
            }
        }
    }

    /// Signal supervisor and join it — this waits out any probe/restart
    /// already in flight rather than racing past it — then stop shadow
    /// with the now-settled state. Call on every clean exit path; Drop
    /// covers whatever this misses.
    pub(crate) async fn shutdown(mut self) {
        self.cancel.cancel();
        if let Some(h) = self.supervisor.take()
            && let Err(e) = h.await
        {
            tracing::warn!(target: "walshadow::shadow", error = %e, "shadow supervisor join failed");
        }
        let guard = &self.guard;
        if guard.keep_running {
            return;
        }
        if let Some(true) = probe_blocking(&guard.shadow, |s| s.is_running()).await
            && probe_blocking(&guard.shadow, |s| s.stop()).await.is_none()
        {
            tracing::warn!(target: "walshadow::shadow", "shadow stop on shutdown failed");
        }
    }
}

/// Run blocking `pg_ctl` operation outside async runtime
/// Return `None` after logging failure
pub(crate) async fn probe_blocking<T: Send + 'static>(
    shadow: &Arc<Shadow>,
    op: impl FnOnce(&Shadow) -> walshadow::shadow::Result<T> + Send + 'static,
) -> Option<T> {
    let s = shadow.clone();
    match tokio::task::spawn_blocking(move || op(&s)).await {
        Ok(Ok(v)) => Some(v),
        Ok(Err(e)) => {
            tracing::warn!(target: "walshadow::shadow", error = %e, "shadow op failed");
            None
        }
        Err(e) => {
            tracing::warn!(target: "walshadow::shadow", error = %e, "shadow op join failed");
            None
        }
    }
}

/// `guard` drops after this, stopping shadow once the supervisor is aborted
impl Drop for ShadowLifecycle {
    fn drop(&mut self) {
        if let Some(h) = &self.supervisor {
            h.abort();
        }
    }
}

/// A shadow missing the applied `[source] dbname` cannot serve its bridge, but
/// a database the source created after the base backup arrives with replay —
/// so this is a warning at startup, and the bridge connect is what refuses
fn warn_database_absent(shadow: &walshadow::shadow::Shadow) {
    if let Ok(false) = shadow.has_database() {
        tracing::warn!(
            target: "walshadow::shadow",
            dbname = shadow.config().dbname.as_str(),
            "shadow holds no such database yet; replay has to create it before the \
             bridge can attach. A data dir provisioned for another source never will",
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::args::args_from;

    #[test]
    fn owned_shadow_sizes_bridge_from_inserter_config() {
        let tmp = tempfile::tempdir().unwrap();
        let args = args_from(&[]);
        for (toml, expect) in [
            (None, Some(1)),
            (Some("[ch]"), None),
            (Some("[ch]\ninserter_pool_size = 5"), Some(5)),
            (Some("[ch]\ninserter_pool_size = 16"), Some(8)),
        ] {
            let config = toml.map(|t| EmitterConfig::from_toml_str(t).unwrap());
            let workers = bridge_pool_size(config.as_ref());
            if let Some(want) = expect {
                assert_eq!(workers, want, "{toml:?}");
            }
            let slots = workers + 1;
            let shadow = build_owned_shadow(
                &args,
                "postgres",
                &["postgres".to_string()],
                tmp.path().to_path_buf(),
                workers,
            );
            let floor = walshadow::shadow::SourceGucFloor {
                max_worker_processes: 1,
                ..Default::default()
            };
            shadow.materialize_conf(&floor, None).unwrap();
            let conf = std::fs::read_to_string(tmp.path().join("postgresql.conf")).unwrap();
            assert!(conf.contains(&format!("walshadow.bridge_workers = {workers}\n")));
            assert!(conf.contains(&format!("max_worker_processes = {slots}\n")));
        }
    }

    #[test]
    fn walsender_conninfo_names_bind_address() {
        let ci = walsender_primary_conninfo("127.0.0.1:5441".parse().unwrap());
        assert!(ci.contains("host=127.0.0.1"), "{ci}");
        assert!(ci.contains("port=5441"), "{ci}");
    }

    /// Shadow whose `pg_ctl` / `pg_controldata` / `psql` are scripts answering
    /// from files under `state`, each call appended to `state/calls`
    struct FakePg {
        _tmp: tempfile::TempDir,
        state: PathBuf,
        bin: PathBuf,
        shadow: Arc<Shadow>,
    }

    impl FakePg {
        fn new() -> Self {
            use std::os::unix::fs::PermissionsExt;
            let tmp = tempfile::tempdir().unwrap();
            let (state, bin) = (tmp.path().join("state"), tmp.path().join("bin"));
            for dir in [&state, &bin, &tmp.path().join("data")] {
                std::fs::create_dir(dir).unwrap();
            }
            let st = state.display();
            let scripts = [
                (
                    "pg_ctl",
                    format!("echo \"pg_ctl $*\" >> {st}/calls\nfor a; do last=$a; done\nexit $(cat {st}/$last)\n"),
                ),
                (
                    "pg_controldata",
                    "for k in max_connections max_worker_processes max_wal_senders max_prepared_xacts max_locks_per_xact; do echo \"$k setting: 20\"; done\n".to_owned(),
                ),
                (
                    "psql",
                    format!(
                        "echo \"psql $*\" >> {st}/calls\ncase \"$*\" in\n*pause_state*) cat {st}/pause ;;\n*current_setting*) cat {st}/running ;;\nesac\nexit $(cat {st}/psql)\n"
                    ),
                ),
            ];
            for (name, body) in scripts {
                let path = bin.join(name);
                std::fs::write(&path, format!("#!/bin/sh\n{body}")).unwrap();
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
            }
            let mut cfg = ShadowConfig::new(tmp.path().join("data"), tmp.path().join("out"));
            cfg.pg_bin_dir = Some(bin.clone());
            cfg.socket_dir = tmp.path().to_path_buf();
            let fake = Self {
                state,
                bin,
                shadow: Arc::new(Shadow::new(cfg)),
                _tmp: tmp,
            };
            for (file, value) in [
                ("status", "0"),
                ("stop", "0"),
                ("start", "0"),
                ("psql", "0"),
                ("pause", "not paused"),
                ("running", "20|20|20|20|20"),
            ] {
                fake.set(file, value);
            }
            fake
        }

        fn set(&self, file: &str, value: &str) {
            std::fs::write(self.state.join(file), value).unwrap();
        }

        fn calls(&self, needle: &str) -> usize {
            let calls = std::fs::read_to_string(self.state.join("calls")).unwrap_or_default();
            calls.lines().filter(|l| l.contains(needle)).count()
        }

        /// Block until `needle` has been called `n` times
        async fn called(&self, needle: &str, n: usize) {
            let poll = async {
                while self.calls(needle) < n {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            };
            tokio::time::timeout(Duration::from_secs(10), poll)
                .await
                .expect(needle);
        }
    }

    #[tokio::test]
    async fn owned_shadow_drop_survives_failed_stop_and_probe() {
        let fake = FakePg::new();
        fake.set("stop", "1");
        drop(OwnedShadow {
            shadow: fake.shadow.clone(),
            keep_running: false,
        });
        assert_eq!(fake.calls("stop"), 1);
        std::fs::remove_file(fake.bin.join("pg_ctl")).unwrap();
        assert!(fake.shadow.is_running().is_err());
        drop(OwnedShadow {
            shadow: fake.shadow.clone(),
            keep_running: false,
        });
    }

    #[tokio::test]
    async fn probe_blocking_reports_panicked_op_as_none() {
        let fake = FakePg::new();
        let none: Option<()> = probe_blocking(&fake.shadow, |_| panic!("op")).await;
        assert!(none.is_none());
    }

    #[tokio::test]
    async fn supervisor_resumes_floor_pause_and_restarts_stopped_shadow() {
        let fake = FakePg::new();
        let pace = SupervisePace {
            probe: Duration::from_millis(1),
            restart: Duration::from_millis(1),
            restart_max: Duration::from_millis(2),
        };
        use tracing::instrument::WithSubscriber;
        use tracing_subscriber::layer::SubscriberExt;
        let messages = crate::housekeeping::tests::Messages::default();
        let cancel = CancellationToken::new();
        let supervise =
            ShadowLifecycle::supervise(fake.shadow.clone(), "host=x".into(), cancel.clone(), pace);
        let task = tokio::spawn(
            supervise.with_subscriber(tracing_subscriber::registry().with(messages.clone())),
        );
        // Floor above running settings is a GUC-floor pause, resumed
        fake.set("pause", "paused");
        fake.set("running", "10|10|10|10|10");
        fake.called("pg_wal_replay_resume", 1).await;
        // Floor equal to running is someone else's pause, left alone
        fake.set("running", "20|20|20|20|20");
        fake.called("pause_state", fake.calls("pause_state") + 1)
            .await;
        let resumed = fake.calls("pg_wal_replay_resume");
        fake.called("pause_state", fake.calls("pause_state") + 2)
            .await;
        assert_eq!(fake.calls("pg_wal_replay_resume"), resumed);
        // Failed probe ignored
        fake.set("psql", "1");
        fake.called("pause_state", fake.calls("pause_state") + 1)
            .await;
        // Stopped postmaster restarts, a failed start backs off and retries
        fake.set("start", "1");
        fake.set("status", "3");
        fake.called(" start", 2).await;
        fake.set("start", "0");
        messages.wait("shadow restarted", 1).await;
        // Status probe that cannot run is skipped, once no restart is in flight
        fake.set("status", "0");
        fake.called("pause_state", fake.calls("pause_state") + 1)
            .await;
        std::fs::remove_file(fake.bin.join("pg_ctl")).unwrap();
        messages
            .wait("shadow op failed", messages.count("shadow op failed") + 1)
            .await;
        cancel.cancel();
        task.await.unwrap();
        let conf = std::fs::read_to_string(fake.shadow.config().data_dir.join("postgresql.conf"));
        assert!(conf.unwrap().contains("max_connections = 20\n"));
    }

    #[tokio::test]
    async fn lifecycle_shutdown_survives_failed_stop() {
        let fake = FakePg::new();
        fake.set("stop", "1");
        let lifecycle = ShadowLifecycle {
            guard: OwnedShadow {
                shadow: fake.shadow.clone(),
                keep_running: true,
            },
            supervisor: Some(tokio::spawn(async { panic!("supervisor") })),
            cancel: CancellationToken::new(),
        };
        lifecycle.shutdown().await;
        assert_eq!(fake.calls("stop"), 0, "keep_running leaves shadow up");
        let lifecycle = ShadowLifecycle::spawn(
            OwnedShadow {
                shadow: fake.shadow.clone(),
                keep_running: false,
            },
            "host=x".into(),
        );
        lifecycle.shutdown().await;
        assert_eq!(fake.calls("stop"), 2, "shutdown stop, then drop's retry");
    }
}
