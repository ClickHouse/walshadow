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
    let socket = socket_dir
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("shadow-socket-dir not UTF-8"))?;
    let conninfo = socket_conninfo(socket, port, user, dbname);
    let (client, conn) = tokio_postgres::connect(&conninfo, tokio_postgres::NoTls)
        .await
        .with_context(|| format!("preflight: open shadow sql client ({conninfo})"))?;
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
        Ok(())
    })
    .await
    .context("shadow start task")?
}

pub(crate) const SHADOW_PROBE_INTERVAL: Duration = Duration::from_secs(2);
pub(crate) const SHADOW_RESTART_BACKOFF_MAX: Duration = Duration::from_secs(60);

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
    ) {
        let mut backoff = Duration::from_secs(1);
        // Edge-trigger the foreign-pause log so a held operator pause does
        // not spam once per tick
        let mut foreign_logged = false;
        loop {
            tokio::select! {
                () = cancel.cancelled() => return,
                () = tokio::time::sleep(SHADOW_PROBE_INTERVAL) => {}
            }
            match probe_blocking(&shadow, |s| s.is_running()).await {
                Some(true) => {
                    backoff = Duration::from_secs(1);
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
                        backoff = Duration::from_secs(1);
                    } else {
                        tokio::select! {
                            () = cancel.cancelled() => return,
                            () = tokio::time::sleep(backoff) => {}
                        }
                        backoff = (backoff * 2).min(SHADOW_RESTART_BACKOFF_MAX);
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
}
