use std::convert::Infallible;
use std::path::PathBuf;
use std::pin::pin;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;

use walshadow_peerdb::control::ControlClient;
use walshadow_peerdb::routes::{App, handle};
use walshadow_peerdb::state::Store;

/// PeerDB flow HTTP API shim over walshadow-control. Default bind matches
/// PeerDB's HTTP gateway port so existing client config carries over
#[derive(Parser)]
#[command(name = "walshadow-peerdb")]
struct Cli {
    #[arg(long, env = "WALSHADOW_PEERDB_BIND", default_value = "0.0.0.0:8113")]
    bind: String,
    /// walshadow-control socket to translate onto
    #[arg(
        long,
        env = "WALSHADOW_CONTROL_SOCKET",
        default_value = "/run/walshadow-control.sock"
    )]
    socket: PathBuf,
    #[arg(long, default_value = "/var/lib/walshadow-peerdb/state.json")]
    state_file: PathBuf,
    /// Shared secret for the Authorization header; unauthenticated when unset
    #[arg(long, env = "PEERDB_PASSWORD", hide_env_values = true)]
    password: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let cli = Cli::parse();
    let app = Arc::new(App {
        control: ControlClient::new(cli.socket),
        store: Store::load(cli.state_file).await?,
        password: cli.password.filter(|p| !p.is_empty()),
        version: format!("walshadow-peerdb-{}", env!("CARGO_PKG_VERSION")),
        stats: Arc::new(walshadow_peerdb::stats::StatsHistory::new()),
    });

    // Sample the daemon's cumulative rows-synced counter on a timer so
    // cdc_graph can serve a sync-history series (the shim keeps no history and
    // the control socket exposes only the live aggregate).
    tokio::spawn({
        let app = app.clone();
        async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(15));
            loop {
                tick.tick().await;
                if let Ok(status) = app.control.call("status", &toml::Table::new()).await {
                    let rows = walshadow_peerdb::handlers::rows_synced_from_status(&status);
                    app.stats.record(walshadow_peerdb::pb::now_unix(), rows);
                }
            }
        }
    });
    let listener = tokio::net::TcpListener::bind(&cli.bind)
        .await
        .with_context(|| format!("bind {}", cli.bind))?;
    tracing::info!(bind = %cli.bind, "peerdb api shim listening");
    let http = hyper::server::conn::http1::Builder::new();
    let mut shutdown = pin!(shutdown_signal());
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => return Ok(()),
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else { continue };
                let app = app.clone();
                let conn = http.serve_connection(
                    TokioIo::new(stream),
                    service_fn(move |req| {
                        let app = app.clone();
                        async move { Ok::<_, Infallible>(handle(&app, req).await) }
                    }),
                );
                tokio::spawn(async move {
                    if let Err(e) = conn.await {
                        tracing::debug!(error = %e, "serve connection");
                    }
                });
            }
        }
    }
}

async fn shutdown_signal() {
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("install SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = sigterm.recv() => {}
    }
}
