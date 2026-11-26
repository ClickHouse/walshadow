//! `walshadow-stream` — full WAL capture pipeline.
//!
//! Connects to source PG in replication mode, `IDENTIFY_SYSTEM` then
//! `START_REPLICATION PHYSICAL` (optionally bound to a permanent slot),
//! filters every WAL byte, writes filtered segments shadow PG reads via
//! `restore_command`.
//!
//! ```text
//! walshadow-stream \
//!     --host /tmp/source_sock --port 5432 --user postgres --dbname postgres \
//!     --shadow-socket-dir /tmp/shadow_sock --shadow-port 5433 \
//!     --bootstrap-shadow-data-dir /var/lib/walshadow/shadow \
//!     --walsender-bind 127.0.0.1:5434 \
//!     --out-dir /var/lib/walshadow/filtered \
//!     --spill-dir /var/lib/walshadow/spill \
//!     [--slot walshadow_phys] \
//!     [--start-lsn 0/16B3750] \
//!     [--metrics-bind 127.0.0.1:9484] \
//!     [--retention-bytes 268435456]
//! ```

#[cfg(not(target_os = "linux"))]
compile_error!(
    "walshadow-stream is supported only on Linux; the PostgreSQL bridge extension may still be built separately with `make -C pgext`"
);

// The pipeline allocates rows on the decode thread(s) and frees them on the
// batcher thread; mimalloc's per-thread caches handle that produce-here/
// free-there pattern far better than glibc's shared arena (which serializes on
// its arena lock under that cross-thread churn).
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(test)]
#[path = "../../../tests/common/tools.rs"]
mod tools;

mod archive;
mod args;
mod bootstrap;
mod housekeeping;
mod metrics_publish;
mod runtime_cfg;
mod session;
mod shadow_proc;
mod sinks;
mod source_db;
mod source_recovery;
mod tracing_setup;

use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use tokio::sync::Mutex;
use walshadow::boundary_hold::AttachSlot;
use walshadow::metrics::MetricsRegistry;
use walshadow::mimalloc;

use crate::args::{
    Args, FailClosedAction, FailClosedArgs, InitArgs, cli_base, url_base, validate_scratch_dir,
    validate_transport_args,
};
use crate::runtime_cfg::spawn_shutdown_signals;
use crate::session::run_session;
use crate::tracing_setup::init_tracing;

fn main() -> Result<()> {
    // `ctl` client mode is detected before daemon-arg parsing so it needn't
    // supply the daemon's required args.
    let argv: Vec<String> = std::env::args().collect();
    if argv.get(1).map(String::as_str) == Some("ctl") {
        let rest = std::iter::once(format!("{} ctl", argv[0])).chain(argv.into_iter().skip(2));
        let (socket, command) = walshadow::ctl::Cli::parse_from(rest).into_parts()?;
        return runtime(NonZeroUsize::new(1))?.block_on(run_ctl(&socket, command));
    }
    if argv.get(1).map(String::as_str) == Some("init") {
        let rest = std::iter::once(format!("{} init", argv[0])).chain(argv.into_iter().skip(2));
        let opts = InitArgs::parse_from(rest).into_opts();
        return runtime(None)?.block_on(async {
            init_tracing(None);
            walshadow::init::run(opts).await
        });
    }
    if argv.get(1).map(String::as_str) == Some("fail-closed") {
        let rest =
            std::iter::once(format!("{} fail-closed", argv[0])).chain(argv.into_iter().skip(2));
        return run_fail_closed(FailClosedArgs::parse_from(rest));
    }
    let args = Args::parse();
    if let Err(e) = runtime(args.worker_threads)?.block_on(daemon(args)) {
        // Same rendering a returned `Err` gets, with a status systemd can
        // tell apart: a fail-closed verdict must not restart
        eprintln!("Error: {e:?}");
        std::process::exit(walshadow::fail_closed::exit_status(&e));
    }
    Ok(())
}

/// `fail-closed show|clear`: operate on the spill-dir marker directly, so
/// it works while the daemon refuses to start
fn run_fail_closed(args: FailClosedArgs) -> Result<()> {
    use walshadow::fail_closed::{EXIT_FAIL_CLOSED, clear_marker, read_marker};
    let dir = args
        .spill_dir
        .context("--spill-dir is required: the daemon's spill directory")?;
    match args.action {
        FailClosedAction::Show => match read_marker(&dir).context("read fail-closed marker")? {
            Some(v) => {
                println!(
                    "reason = {}\nlsn = {}\nat_unix = {}\nerror = {}",
                    v.reason,
                    v.lsn_text(),
                    v.at_unix,
                    v.error
                );
                std::process::exit(EXIT_FAIL_CLOSED);
            }
            None => println!("no fail-closed marker in {}", dir.display()),
        },
        FailClosedAction::Clear => match clear_marker(&dir).context("clear fail-closed marker")? {
            Some(v) => println!("cleared {v}"),
            None => println!("no fail-closed marker in {}", dir.display()),
        },
    }
    Ok(())
}

/// Stop terminally on `verdict`: publish it on the metrics and status
/// surfaces, optionally hold them up until shutdown, and return the error
/// `main` maps onto the fail-closed exit status
async fn stop_fail_closed(
    args: &Args,
    metrics: &MetricsRegistry,
    shutdown: &tokio_util::sync::CancellationToken,
    verdict: walshadow::fail_closed::FailClosed,
) -> anyhow::Error {
    metrics
        .update(|snap| snap.fail_closed = Some(verdict.clone()))
        .await;
    if args.fail_closed_hold {
        tracing::error!(
            target: "walshadow",
            reason = %verdict.reason,
            lsn = %verdict.lsn_text(),
            "fail-closed; holding metrics and control surfaces until SIGTERM",
        );
        shutdown.cancelled().await;
    }
    walshadow::fail_closed::FailClosedExit::new(verdict, &args.spill_dir).into()
}

fn runtime(workers: Option<NonZeroUsize>) -> std::io::Result<tokio::runtime::Runtime> {
    let mut rt = tokio::runtime::Builder::new_multi_thread();
    if let Some(n) = workers {
        rt.worker_threads(n.get());
    }
    rt.enable_all().build()
}

async fn daemon(args: Args) -> Result<()> {
    walshadow::trace::set_sample_ratio(args.trace_sample_ratio);
    // `--otlp-endpoint` wins; otherwise honor the conventional env var.
    let otlp_endpoint = args
        .otlp_endpoint
        .clone()
        .or_else(|| std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok());
    let tracer_provider = init_tracing(otlp_endpoint.as_deref());
    let result = run(args).await;
    // The batch span processor lives on a background thread, so a bare
    // process exit drops whatever it hasn't flushed. Drain it before we
    // return (best-effort — a failed flush must not mask `run`'s result).
    if let Some(provider) = tracer_provider
        && let Err(e) = provider.shutdown()
    {
        tracing::warn!(target: "walshadow", error = %e, "otlp tracer shutdown");
    }
    result
}

async fn run_ctl(socket: &Path, cmd: walshadow::ctl::Command) -> Result<()> {
    use std::io::{IsTerminal, Read};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let body = if cmd.reads_stdin && !std::io::stdin().is_terminal() {
        let mut raw = String::new();
        std::io::stdin().read_to_string(&mut raw)?;
        raw.parse().context("parse config body as TOML")?
    } else {
        cmd.body
    };
    let doc = walshadow::control::encode_request(&cmd.verb, body)?;
    let mut stream = tokio::net::UnixStream::connect(socket)
        .await
        .context(format!("connect control socket {}", socket.display()))?;
    stream.write_all(doc.as_bytes()).await?;
    stream.flush().await?;
    stream.shutdown().await.ok();
    let mut resp = String::new();
    stream.read_to_string(&mut resp).await?;
    let (head, payload) = resp.split_once('\n').unwrap_or((resp.as_str(), ""));
    let Some(trailer) = head.strip_prefix("OK") else {
        eprint!("{resp}");
        std::process::exit(1);
    };
    if !trailer.trim().is_empty() {
        println!("{}", trailer.trim());
    }
    let rendered = walshadow::ctl::render(&cmd.verb, payload);
    if !rendered.trim().is_empty() {
        println!("{}", rendered.trim_end());
    }
    Ok(())
}

/// Process-lifetime entry: bind metrics + control socket + SIGHUP, then stream
/// one session. Every reconfigure (socket / SIGHUP) is a live reload — no
/// restart. SIGINT/SIGTERM break the pump loop and drain gracefully.
async fn run(mut args: Args) -> Result<()> {
    use walshadow::control::{Reloader, SharedCtx};

    args.url_base = url_base(&args)?;
    validate_transport_args(&args)?;
    validate_scratch_dir(&args)?;
    let sighup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
        .context("install SIGHUP")?;
    let shutdown = spawn_shutdown_signals()?;

    let metrics = MetricsRegistry::new();
    let reloader = Arc::new(Reloader::default());

    let _metrics_server = if let Some(addr) = args.metrics_bind {
        let (bound, h) = walshadow::metrics::serve(addr, metrics.clone())
            .await
            .context("bind metrics endpoint")?;
        tracing::info!(target: "walshadow::metrics", addr = %bound, "metrics endpoint serving");
        Some(h)
    } else {
        None
    };

    let _control_server = if let Some(sock) = args.control_socket.clone() {
        let ch_config = args
            .ch_config
            .clone()
            .context("--control-socket requires --ch-config")?;
        let ctx = SharedCtx {
            fragment: walshadow::control::fragment_path(&ch_config, &args.control_fragment)?,
            ch_config,
            cli_base: cli_base(&args),
            metrics: metrics.clone(),
            reloader: reloader.clone(),
            frag_lock: Arc::new(Mutex::new(())),
        };
        Some(walshadow::control::serve(sock, ctx).context("bind control socket")?)
    } else {
        None
    };

    // A deterministic verdict persisted by an earlier run: refuse before
    // touching WAL, which would only reach the same verdict again
    if let Some(verdict) =
        walshadow::fail_closed::read_marker(&args.spill_dir).context("read fail-closed marker")?
    {
        tracing::error!(
            target: "walshadow",
            marker = %args.spill_dir.join(walshadow::fail_closed::MARKER_FILE).display(),
            "refusing to start: spill directory holds a fail-closed verdict",
        );
        return Err(stop_fail_closed(&args, &metrics, &shutdown, verdict).await);
    }

    let fail_closed = walshadow::fail_closed::FailClosedLatch::default();
    let mut sighup = sighup;
    loop {
        let attach: AttachSlot = Arc::default();
        let result = run_session(
            &args,
            &metrics,
            &reloader,
            sighup,
            &shutdown,
            &attach,
            &fail_closed,
        )
        .await;
        if let Some(verdict) = fail_closed.get() {
            walshadow::fail_closed::write_marker(&args.spill_dir, &verdict)
                .await
                .context("persist fail-closed marker")?;
            let session_err = result.err().map(|e| format!("{e:#}"));
            tracing::error!(
                target: "walshadow",
                session_error = session_err.as_deref().unwrap_or(""),
                "session ended on a fail-closed verdict; marker written",
            );
            return Err(stop_fail_closed(&args, &metrics, &shutdown, verdict).await);
        }
        let Some(attached) = attach.lock().expect("attach slot poisoned").take() else {
            return result;
        };
        tracing::info!(
            target: "walshadow",
            database = %attached.name,
            oid = attached.oid,
            lsn = format_args!("{:#X}", attached.lsn),
            "restarting the session to follow a database the shadow now holds",
        );
        args.ignore_cursor = false;
        args.start_lsn = None;
        sighup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
            .context("reinstall SIGHUP")?;
    }
}
