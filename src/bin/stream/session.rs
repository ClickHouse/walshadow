//! One streaming session — source connect through pump loop, shutdown, and
//! everything the status tick publishes along the way.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use backon::{ExponentialBuilder, Retryable};
use foldhash::{HashMap, HashMapExt};
use tokio::sync::{Mutex, watch};
use tokio_postgres::types::Oid;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use walrus::pg::backup::format_pg_lsn;
use walshadow::archive::Archive;
use walshadow::boundary_hold::{
    AttachSlot, BoundaryGateConfig, BoundaryHoldSink, CatalogBoundaryGate, PendingDatabase,
    PendingDatabases,
};
use walshadow::ch_emitter::{EmitterConfig, EmitterStats};
use walshadow::config::{ConfigResolver, SourceConn};
#[cfg(feature = "test-support")]
use walshadow::crash_test;
use walshadow::manifest;
use walshadow::metrics::{MetricsRegistry, RateEstimator};
use walshadow::pg::socket_conninfo;
use walshadow::pipeline::{PipelineConfig, TailKind};
use walshadow::pos::{
    EmitterAck, FilterDurable, Floor, Monotone, Pos, RawStart, ShadowFlush, ShadowReplay,
    SourceReceived,
};
use walshadow::queueing_record_sink::{
    DEFAULT_QUEUEING_BATCH_SIZE, DEFAULT_QUEUEING_RECORD_SINK_CAPACITY, QueueingRecordSink,
};
use walshadow::record::{MetricsRecordSink, WAL_SEG_SIZE};
use walshadow::retention::{DEFAULT_TRIM_INTERVAL, max_segment_end, remove_stale_temps};
use walshadow::segment_sink::{DirSegmentSink, SegFsync};
use walshadow::source_db::{DbLink, DbLinkConfig, SourceDb, SourceDbs};
use walshadow::source_feed::{SourceEvent, SourceFeed, StandbyStatus};
use walshadow::timeline::TimelineHistory;
use walshadow::trace::TxnSpanRegistry;
use walshadow::transition::{
    CrossingState, ForkGuards, PrefixOrigin, Switchover, TimelineStats, load_boot_history,
    seed_shadow_branches,
};
use walshadow::wal_stream::WalStream;
use walshadow::xact_buffer::{BufferingDecoderSink, SubxactTracker, XactBuffer, XactBufferConfig};

use crate::args::{Args, cli_base, finish_ch_config, positive_usize};
use crate::bootstrap::{
    BootstrapHandoff, BootstrapMetrics, BootstrapObservers, ShadowStart, resolve_bootstrap,
    resolve_shadow_start, run_bootstrap,
};
use crate::housekeeping::{
    SEGMENT_FSYNC_QUEUE, spawn_desc_log_gc, spawn_segment_fsync, trim_retention,
};
use crate::metrics_publish::{
    DbMetricSources, DrainResident, PumpClock, ShadowMetricsView, SourceSwap, StageCounters,
    TimelineView, merge_worker_seconds, pipeline_worker_seconds, populate_metrics,
    shadow_apply_lag,
};
use crate::runtime_cfg::{or_signal, sighup_reload};
use crate::shadow_proc::{
    OwnedShadow, ShadowLifecycle, bridge_pool_size, build_owned_shadow, open_shadow_sql_client,
    probe_blocking, start_owned_shadow, walsender_primary_conninfo,
};
use crate::sinks::{DaemonSinks, DecoderXactPair};
use crate::source_db::{
    ATTACH_MARKER, DescLogInputs, SourceDbInputs, build_source_db, list_databases, metrics_only_db,
    open_db_desc_log, open_source_sql_client,
};
use crate::source_recovery::{
    BARRIER_LOG_INTERVAL, PROMOTION_POLL, PauseState, PromotionGate, ReconnectBackoff, ResumePoint,
    SOURCE_SWAP_RETRY, SourceModeStats, SourcePath, SourceRecovery, commit_fork_resume,
    connect_source_waiting, promotion_gate, resume_manifest, resume_source_feed, stream_branch,
    stream_error_kind, swap_reason,
};

const CONFIG_LOG_INTERVAL: Duration = Duration::from_secs(3600);

/// Wait out an unreachable ClickHouse at boot rather than exit, which leaves
/// a supervisor restart-looping the daemon until the destination returns
async fn ensure_boot_database_waiting(cfg: &EmitterConfig) -> Result<()> {
    (|| walshadow::ch_ddl::ensure_boot_database(cfg))
        .retry(
            ExponentialBuilder::default()
                .with_min_delay(Duration::from_millis(200))
                .with_max_delay(Duration::from_secs(10))
                .without_max_times(),
        )
        .when(walshadow::ch::is_retryable)
        .notify(|e, _| {
            tracing::warn!(
                target: "walshadow",
                error = %e,
                host = %cfg.conn.host,
                port = cfg.conn.port,
                "ClickHouse unreachable, waiting for it",
            );
        })
        .await?;
    Ok(())
}

/// Poll until a walreceiver streams from the walsender, or `timeout` passes
async fn await_walreceiver(
    shadow_state: &Mutex<walshadow::shadow_stream::ShadowStreamState>,
    walsender_addr: std::net::SocketAddr,
    timeout: Duration,
    shutdown: &CancellationToken,
) -> Result<()> {
    let start = Instant::now();
    loop {
        let agg = shadow_state.lock().await.aggregate();
        if agg.active_connections > 0 {
            break;
        }
        // `accepted` separates "shadow never dialed" from "shadow dialed
        // and stalled in the handshake" — the latter reads as the former
        // without it, since only START_REPLICATION registers a connection
        let (secs, accepted) = (timeout.as_secs(), agg.accepted());
        anyhow::ensure!(
            start.elapsed() < timeout,
            "no walreceiver streaming from walsender {walsender_addr} within \
             {secs}s (accepted {accepted}, none sent START_REPLICATION); catalog-boundary \
             holds require a live wire — point shadow's primary_conninfo here \
             or raise --walsender-connect-timeout",
        );
        or_signal(shutdown, async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(())
        })
        .await?;
    }
    tracing::info!(
        target: "walshadow",
        wait = ?start.elapsed(),
        "walsender connected — starting pump",
    );
    Ok(())
}

/// Followed databases in bridge socket order: `[source] dbname` plus every
/// database-prefixed config key
fn followed_databases(ch_config: Option<&EmitterConfig>, dbname: &str) -> Result<Vec<String>> {
    let databases = match ch_config {
        Some(cfg) if !cfg.databases.is_empty() => cfg.databases.clone(),
        _ => vec![dbname.to_owned()],
    };
    let seats = walshadow::bridge::MAX_BRIDGE_DATABASES;
    let named = databases.len();
    anyhow::ensure!(
        named <= seats,
        "config names {named} source databases, the shadow bridge seats {seats}"
    );
    Ok(databases)
}

/// Manifest boot resumes from. Unreadable is fatal unless an operator override
/// discards it, another cluster's is fatal regardless
fn boot_manifest(
    loaded: Result<Option<manifest::Manifest>, manifest::ManifestError>,
    overridden: bool,
    spill_dir: &std::path::Path,
) -> Result<Option<manifest::Manifest>> {
    match loaded {
        Ok(m) => Ok(m),
        Err(e @ manifest::ManifestError::ForeignSource { .. }) => Err(e.into()),
        Err(e) if overridden => {
            tracing::warn!(
                target: "walshadow::manifest",
                error = %e,
                spill_dir = %spill_dir.display(),
                "manifest unreadable; operator override discards it",
            );
            Ok(None)
        }
        Err(e) => anyhow::bail!(
            "manifest at {} unreadable: {e}; restore it, or authorize \
             recovery with --ignore-cursor / --start-lsn",
            manifest::manifest_path(spill_dir).display(),
        ),
    }
}

/// Branch serving `aligned` on the source's history. `--ignore-cursor`
/// adopts the live timeline when the stored one has no lineage there
fn start_branch(
    history: &mut TimelineHistory,
    stored_timeline: u32,
    aligned: Pos<Floor>,
    live_timeline: u32,
    ignore_cursor: bool,
) -> Result<u32> {
    if let Some(tli) = history.resume_branch(stored_timeline, aligned.get(), WAL_SEG_SIZE) {
        return Ok(tli);
    }
    let serves = history.tli_of_segment(aligned.get(), WAL_SEG_SIZE);
    anyhow::ensure!(
        ignore_cursor,
        "timeline_not_descendant: stored timeline {stored_timeline} does not reach \
         {aligned} on live timeline {live_timeline}'s history (it serves {serves:?}); \
         --ignore-cursor re-baselines onto the live branch",
    );
    tracing::warn!(
        target: "walshadow",
        stored_timeline,
        live_timeline,
        serves_start = serves,
        "--ignore-cursor adopts the live timeline without a lineage proof",
    );
    *history = TimelineHistory::root(live_timeline);
    Ok(live_timeline)
}

/// Same number, different branch: only the switchpoint tells a sibling from
/// the branch artifacts came off. `stored_begin` 0 is a manifest that never
/// recorded one
fn check_branch_begin(
    stored_timeline: u32,
    stored_begin: u64,
    live_begin: u64,
    ignore_cursor: bool,
) -> Result<()> {
    match stored_begin {
        0 if stored_timeline > 1 => tracing::warn!(
            target: "walshadow",
            stored_timeline,
            live_begin = %format_pg_lsn(live_begin),
            "manifest records no switchpoint for its branch, so a sibling sharing \
             that number cannot be refused until the next manifest write",
        ),
        0 => {}
        begin if begin != live_begin => {
            anyhow::ensure!(
                ignore_cursor,
                "sibling_branch: source places timeline {stored_timeline} at {}, \
                 walshadow's artifacts came off it from {}; the branch behind them is \
                 absent from this source's history",
                format_pg_lsn(live_begin),
                format_pg_lsn(begin),
            );
            tracing::warn!(
                target: "walshadow",
                stored_timeline,
                stored_begin = %format_pg_lsn(begin),
                live_begin = %format_pg_lsn(live_begin),
                "--ignore-cursor adopts a branch that begins somewhere else",
            );
        }
        _ => {}
    }
    Ok(())
}

/// Wait out tracked I/O on every exit, attach restart reopens same files
/// in-process
pub(crate) async fn run_session(
    args: &Args,
    metrics: &MetricsRegistry,
    reloader: &Arc<walshadow::control::Reloader>,
    sighup: tokio::signal::unix::Signal,
    shutdown: &CancellationToken,
    attach: &AttachSlot,
    fail_closed: &walshadow::fail_closed::FailClosedLatch,
) -> Result<()> {
    let io = TaskTracker::new();
    let res = session_body(
        args,
        metrics,
        reloader,
        sighup,
        shutdown,
        attach,
        fail_closed,
        &io,
    )
    .await;
    io.close();
    io.wait().await;
    res
}

#[allow(clippy::too_many_arguments)]
async fn session_body(
    args: &Args,
    metrics: &MetricsRegistry,
    reloader: &Arc<walshadow::control::Reloader>,
    mut sighup: tokio::signal::unix::Signal,
    shutdown: &CancellationToken,
    attach: &AttachSlot,
    fail_closed: &walshadow::fail_closed::FailClosedLatch,
    io: &TaskTracker,
) -> Result<()> {
    // Clone the Arc-backed registry so the body's `&metrics` uses are unchanged.
    let metrics = metrics.clone();
    let mut tasks = SessionTasks::default();
    let sighup = futures::stream::poll_fn(move |cx| sighup.poll_recv(cx));
    tasks.spawn("sighup reload", sighup_reload(sighup, reloader.clone()));

    let merged: toml::Table = match args.ch_config.as_deref() {
        Some(p) => walshadow::ch_emitter::load_effective(p, cli_base(args))
            .await
            .context(format!("load config {}", p.display()))?,
        None => cli_base(args),
    };
    // Applied source endpoint. Boot resolves it file-over-CLI; a later reload
    // republishes it on the config watch and the pump swaps its feed.
    let mut source_conn =
        SourceConn::from_table(&merged).map_err(|e| anyhow::anyhow!("[source] {e}"))?;
    if args.slot.is_some() {
        source_conn.slot = args.slot.clone();
    }
    let mut cfg = source_conn.to_pg_config();
    metrics
        .update(|snap| snap.source_mode = walshadow::metrics::SourceMode::Reconnecting.one_hot())
        .await;
    let mut feed = or_signal(
        shutdown,
        connect_source_waiting(args, &mut source_conn, &mut cfg, SOURCE_SWAP_RETRY),
    )
    .await?;

    let ident = feed.identify_system().await.context("IDENTIFY_SYSTEM")?;
    tracing::info!(
        target: "walshadow",
        sysid = ident.system_id,
        timeline = ident.timeline,
        xlogpos = format_pg_lsn(ident.xlogpos).to_string(),
        "source identified",
    );

    // `[ch]` presence decides emitter vs metrics-only.
    let ch_config = if merged.contains_key("ch") {
        let cfg = finish_ch_config(
            EmitterConfig::from_table(&merged).context("parse ch config")?,
            args,
        );
        if cfg.databases.len() > 1 {
            tracing::info!(
                target: "walshadow::config",
                dbname = %cfg.source.dbname,
                databases = ?cfg.databases,
                "following several source databases",
            );
        }
        Some(cfg)
    } else {
        None
    };
    // Before anything dials CH naming that database in its handshake — the
    // bootstrap insert tail is first, and its failure there reads as a
    // bootstrap fault rather than a missing destination
    if let Some(cfg) = ch_config.as_ref() {
        or_signal(shutdown, ensure_boot_database_waiting(cfg)).await?;
    }
    // QueueingRecordSink knobs feed both the CH and metrics-only pipelines,
    // so resolve here while `ch_config` is still in scope (it is consumed
    // into `emitter_cfg` below). CLI over `[ch]` over the built-in default.
    let decoder_batch_size = positive_usize(
        "decoder_batch_size",
        args.decoder_batch_size,
        ch_config
            .as_ref()
            .map_or(DEFAULT_QUEUEING_BATCH_SIZE, |c| c.decoder_batch_size),
    );
    let decoder_queue_capacity = positive_usize(
        "decoder_queue_capacity",
        args.decoder_queue_capacity,
        ch_config
            .as_ref()
            .map_or(DEFAULT_QUEUEING_RECORD_SINK_CAPACITY, |c| {
                c.decoder_queue_capacity
            }),
    );
    let source_databases = followed_databases(ch_config.as_ref(), &source_conn.dbname)?;
    let bootstrap_plan = resolve_bootstrap(args, ch_config.as_ref())?;
    let shadow_start = resolve_shadow_start(args, bootstrap_plan.mode)?;
    if ch_config.as_ref().is_some_and(|c| c.toast.mode.is_shadow())
        && let ShadowStart::Resume(dir) | ShadowStart::Handoff(dir, _) = &shadow_start
    {
        walshadow::filter::shadow_relations::ShadowRelations::load(dir).await?;
    }
    let bridge_workers = bridge_pool_size(ch_config.as_ref());
    // Slot before bootstrap
    if let Some(slot) = source_conn.slot.as_deref() {
        feed.ensure_physical_slot(slot)
            .await
            .context(format!("ensure physical replication slot {slot}"))?;
        tracing::info!(target: "walshadow", slot, "physical replication slot ready");
    }
    // Uptime anchors here, ahead of bootstrap: an initial load is part of the
    // session, and a `t0` that only starts at the status loop reads as a
    // counter reset to a scraper watching through it
    let start_instant = Instant::now();
    // One emitter-counter handle for both phases. Bootstrap's insert tail and
    // the streaming pipeline write the same series, so sharing it is what
    // keeps inserter and TOAST totals from resetting at handoff
    let emitter_stats = Arc::new(EmitterStats {
        fail_closed: fail_closed.clone(),
        ..EmitterStats::default()
    });
    let mut bootstrap_metrics: Option<BootstrapMetrics> = None;
    let mut bootstrap_handoff = if let ShadowStart::Bootstrap(_, attempt) = &shadow_start {
        if !args.skip_preflight {
            let source_sql = feed
                .sql_client()
                .await
                .context("source sidecar sql for bootstrap pre-flight")?;
            walshadow::preflight::bootstrap(walshadow::preflight::BootstrapInputs {
                source_sql,
                wal_from_archive: args.bootstrap_wal_from_archive,
                window_leg: bootstrap_plan.live_window_leg(args),
            })
            .await
            .context("bootstrap pre-flight probe")?
            .into_result()
            .context("pre-flight rejected bootstrap")?;
        }
        let (handoff, stage) = or_signal(
            shutdown,
            run_bootstrap(
                &cfg,
                &mut feed,
                args,
                &bootstrap_plan,
                attempt.clone(),
                ch_config.clone(),
                BootstrapObservers {
                    metrics: &metrics,
                    emitter_stats: emitter_stats.clone(),
                    uptime_from: start_instant,
                },
            ),
        )
        .await
        .context("bootstrap")?;
        bootstrap_metrics = Some(stage);
        Some(handoff)
    } else if let ShadowStart::Handoff(_, handoff) = &shadow_start {
        Some(BootstrapHandoff::new(*handoff, None))
    } else {
        None
    };
    let bootstrap_end_lsn: Option<u64> = bootstrap_handoff.as_ref().map(|h| h.end_lsn);
    let bootstrap_resume_lsn: Option<u64> =
        bootstrap_handoff.as_ref().map(BootstrapHandoff::resume_lsn);
    let bootstrap_timeline: Option<u32> = bootstrap_handoff.as_ref().map(|h| h.timeline);
    // Regenerate config because shadow's port, socket, and GUC floor may change
    // Keep shadow alive until pipeline teardown finishes
    // Reuse shadow instance started during bootstrap
    let owned = match bootstrap_handoff.as_mut().and_then(|h| h.shadow.take()) {
        Some(running) => running,
        None => {
            let owned = OwnedShadow::new(
                build_owned_shadow(
                    args,
                    &source_conn.dbname,
                    &source_databases,
                    shadow_start.data_dir().to_path_buf(),
                    bridge_workers,
                ),
                args.keep_shadow_running,
            );
            owned
                .shadow
                .write_standby_signal()
                .context("write standby.signal")?;
            or_signal(
                shutdown,
                walshadow::ops::stages::SHADOW_REPLAY.measure(start_owned_shadow(
                    &owned.shadow,
                    bootstrap_end_lsn,
                    args.keep_shadow_running,
                )),
            )
            .await?;
            owned
        }
    };
    let stand_in = stand_in_primary(&owned.shadow, &source_conn.dbname).await;
    let (owned, source_databases) = if let Some(name) = &stand_in {
        let substituted = substitute_database(&source_databases, &source_conn.dbname, name);
        tracing::warn!(
            target: "walshadow",
            absent = source_conn.dbname.as_str(),
            stand_in = name.as_str(),
            "shadow holds no such database; following {} until replay creates it",
            name,
        );
        owned
            .shadow
            .stop()
            .context("stop shadow to restand its bridge")?;
        let owned = OwnedShadow::new(
            build_owned_shadow(
                args,
                name,
                &substituted,
                shadow_start.data_dir().to_path_buf(),
                bridge_workers,
            ),
            args.keep_shadow_running,
        );
        owned
            .shadow
            .write_standby_signal()
            .context("write standby.signal")?;
        or_signal(
            shutdown,
            start_owned_shadow(&owned.shadow, None, args.keep_shadow_running),
        )
        .await?;
        (owned, substituted)
    } else {
        (owned, source_databases)
    };
    let shadow_lifecycle =
        ShadowLifecycle::spawn(owned, walsender_primary_conninfo(args.walsender_bind));
    let archive = ch_config
        .as_ref()
        .and_then(|c| c.backup.clone())
        .map(Archive::open)
        .transpose()
        .context("open backup archive")?;
    let start_lsn_override: Option<Pos<RawStart>> = args.start_lsn.map(Pos::new);

    let live_identity = manifest::SourceIdentity {
        system_id: ident.system_id,
        timeline: ident.timeline,
        timeline_begin: Pos::ZERO,
    };
    // Identity gate runs before `--ignore-cursor`: the flag discards resume
    // LSNs, not artifact ownership. Foreign system_id is fatal regardless
    // (retire/backfill ledgers would act on another cluster's state). A newer
    // live timeline is a promotion, proved against the source's history below.
    let loaded = manifest::load(&args.spill_dir, &live_identity).await;
    let overridden = args.ignore_cursor || start_lsn_override.is_some();
    let manifest_at_boot = boot_manifest(loaded, overridden, &args.spill_dir)?;
    // Precedence: explicit > bootstrap > manifest > greenfield head
    let manifest_at_boot = if args.ignore_cursor {
        None
    } else {
        manifest_at_boot
    };
    let raw_start = manifest::resolve_resume_lsn(
        start_lsn_override,
        bootstrap_resume_lsn.map(Pos::new),
        manifest_at_boot.as_ref().map(|m| m.lsn.emitter_ack),
        Pos::new(ident.xlogpos),
    );
    let pinned = bootstrap_end_lsn.is_some() || start_lsn_override.is_some();
    let shadow_holds_data = ch_config.as_ref().is_some_and(|c| c.toast.mode.is_shadow());
    let shadow_replay_seed = manifest_at_boot
        .as_ref()
        .map(|m| m.lsn.shadow_replay.get().max(m.lsn.shadow_flush.get()))
        .unwrap_or_default();
    let boot_shadow_floor = if pinned {
        manifest::ShadowFloor::unbounded()
    } else {
        manifest::ShadowFloor::new(shadow_holds_data, 0, shadow_replay_seed)
    };
    let raw_start = boot_shadow_floor.bound(raw_start);
    let floor_at_boot = manifest_at_boot
        .as_ref()
        .map(|m| m.floor)
        .filter(|f| !f.is_zero());
    // Archive-end scan only feeds the greenfield clamp (keep archive
    // continuous until live streaming begins: starting after last sealed
    // segment leaves shadow missing WAL; re-read from earlier LSN, CH
    // removes duplicates using `_lsn`). A persisted floor folded the clamp
    // at write time.
    let archive_end = if !pinned && floor_at_boot.is_none() {
        max_segment_end(&args.out_dir)
            .await
            .context("scan out-dir for sealed archive end")?
    } else {
        None
    };
    let aligned = manifest::resolve_start(
        raw_start,
        floor_at_boot,
        pinned,
        archive_end,
        boot_shadow_floor,
    );
    tracing::info!(
        target: "walshadow",
        raw = %raw_start,
        aligned = %aligned,
        from_bootstrap = bootstrap_end_lsn.is_some() && args.start_lsn.is_none(),
        from_floor = floor_at_boot.is_some() && !pinned,
        "start LSN",
    );

    // Branch selection is per segment, through the source's history: a floor
    // stored on an ancestor is served by that ancestor, whatever the live head
    // reports, and a floor at a fork segment's start is served by the descendant
    // whose file holds the ancestor prefix (architecture/recovery.md).
    let stored_timeline = bootstrap_timeline
        .or_else(|| manifest_at_boot.as_ref().map(|m| m.source.timeline))
        .unwrap_or(ident.timeline);
    let mut history = load_boot_history(&mut feed, ident.timeline, stored_timeline).await?;
    let start_timeline = start_branch(
        &mut history,
        stored_timeline,
        aligned,
        ident.timeline,
        args.ignore_cursor,
    )?;
    // Backup passes replay archived WAL off the same branch, and outlive a
    // crossing, so they read the chain here rather than re-deriving it
    let (history_tx, history_rx) = watch::channel(Arc::new(history.clone()));
    // Same number, different branch: the chain places a sibling exactly where it
    // places a descendant, and only the switchpoint separates them. A stored
    // begin is the chain a previous run proved, carried forward
    // (architecture/recovery.md)
    let live_begin = history.begin_of(stored_timeline).unwrap_or(0);
    let stored_begin = if bootstrap_timeline.is_some() {
        live_begin
    } else {
        manifest_at_boot
            .as_ref()
            .map(|m| m.source.timeline_begin.get())
            .unwrap_or(0)
    };
    check_branch_begin(
        stored_timeline,
        stored_begin,
        live_begin,
        args.ignore_cursor,
    )?;
    if start_timeline != ident.timeline {
        tracing::info!(
            target: "walshadow",
            start_timeline,
            live_timeline = ident.timeline,
            switch_lsn = history
                .switchpoint_of(start_timeline)
                .map(|l| format_pg_lsn(l).to_string()),
            "resuming on an ancestor timeline; the crossing follows its fork",
        );
    }
    // Branches a spill-dir artifact may carry: the resume branch plus every
    // ancestor the chain places below it. A crossing moves the resume branch
    // while the artifacts stay where they were written
    let lineage: Vec<u32> = history
        .entries()
        .iter()
        .map(|e| e.tli)
        .filter(|tli| *tli <= start_timeline)
        .collect();

    let mut stream = WalStream::builder(start_timeline, WAL_SEG_SIZE, aligned)?;
    let prefix_dirs = [args.out_dir.clone(), shadow_start.data_dir().join("pg_wal")];
    stream.preserve_resume_prefix(&prefix_dirs).await?;
    // Shadow must attach to this listener before catalog replay can advance
    let mut shadow_boot = walshadow::shadow_stream::ShadowStreamState::new(
        history.shadow_boot_branch(stored_timeline, aligned.get(), start_timeline),
        ident.system_id.to_string(),
        aligned.get(),
        args.walsender_slow_threshold,
    );
    seed_shadow_branches(&mut shadow_boot, &history, &args.out_dir, start_timeline).await?;
    let shadow_state = Arc::new(Mutex::new(shadow_boot));
    let walsender_addr = args.walsender_bind;
    let walsender_task = walshadow::shadow_stream::spawn_listener(
        walshadow::shadow_stream::WalSenderAddr::Tcp(walsender_addr),
        shadow_state.clone(),
        Duration::from_millis(50),
    )
    .await
    .context(format!("bind walsender at {walsender_addr}"))?;
    tasks.adopt("walsender listener", walsender_task);
    tracing::info!(target: "walshadow", addr = %walsender_addr, "walsender listening");
    stream.set_bytes_sink(Box::new(walshadow::shadow_stream::ShadowStreamSink::new(
        shadow_state.clone(),
    )));
    // Set address after bind so first connection succeeds
    // Supervisor restarts a shadow that is down, with the address in its conf
    let conninfo = walsender_primary_conninfo(walsender_addr);
    probe_blocking(&shadow_lifecycle.guard.shadow, move |s| {
        s.point_at_walsender(&conninfo)
    })
    .await;

    // Seed catalog tracker from source's current pg_class before
    // START_REPLICATION. Closes the "source rotated a mapped catalog above
    // 16384 pre-attach" hole the < 16384 bootstrap rule misses. Idempotent.
    {
        let sql_client = feed
            .sql_client()
            .await
            .context("open sidecar sql client for seed_from_source")?;
        let added = stream
            .filter_mut()
            .tracker_mut()
            .seed_from_source(sql_client)
            .await
            .context("seed_from_source")?;
        let observed_from = stream
            .filter_mut()
            .seed_observed_from_source(sql_client)
            .await
            .context("seed observed-from xid")?;
        tracing::info!(
            target: "walshadow",
            observed_from,
            "transactions from this xid on are observed whole",
        );
        tracing::info!(
            target: "walshadow",
            added,
            "seeded catalog filenodes from source pg_class"
        );
    }

    // Connect bridge and shadow catalog before START_REPLICATION so the
    // tracker→drain wire is hot from the first record. One pair per followed
    // database: catalog reads and value conversion answer from the database
    // that wrote the bytes
    let connect_budget = Duration::from_secs(args.shadow_connect_timeout);
    let bridge_path = args.bridge_socket_path();
    let socket_dir = args
        .shadow_socket_dir
        .to_str()
        .context("shadow-socket-dir not UTF-8")?;
    let primary_dbname = stand_in
        .clone()
        .unwrap_or_else(|| source_conn.dbname.clone());
    let primary_position = source_databases
        .iter()
        .position(|db| *db == primary_dbname)
        .unwrap_or(0);
    let link_for = |index: usize| -> Result<_> {
        let name = &source_databases[index];
        let emitter = match ch_config.as_ref() {
            Some(_) => Some(finish_ch_config(
                EmitterConfig::for_database(&merged, name)
                    .context(format!("parse ch config for database {name}"))?,
                args,
            )),
            None => None,
        };
        let conninfo = socket_conninfo(socket_dir, args.shadow_port, &args.shadow_user, name);
        Ok((name, emitter, conninfo))
    };
    let mut links: Vec<Option<DbLink>> = (0..source_databases.len()).map(|_| None).collect();
    {
        let (name, emitter, conninfo) = link_for(primary_position)?;
        links[primary_position] = Some(
            DbLink::connect(DbLinkConfig {
                name,
                index: primary_position,
                workers: bridge_workers,
                bridge_path: &bridge_path,
                shadow_conninfo: &conninfo,
                budget: connect_budget,
                emitter,
            })
            .await?,
        );
    }
    let mut pending_dbs: Vec<PendingDatabase> = Vec::new();
    if stand_in.is_some() {
        tokio::fs::write(args.spill_dir.join(ATTACH_MARKER), b"")
            .await
            .context(format!("mark {} as attaching", args.spill_dir.display()))?;
        let source_sql = feed
            .sql_client()
            .await
            .context("source sidecar sql for the absent database")?;
        let visible_at = walshadow::pg::current_wal_lsn(source_sql)
            .await
            .context("source WAL position for the absent database")?;
        let dbname = &source_conn.dbname;
        let absent = format!("database {dbname} is configured but not on the source");
        let oid = *list_databases(source_sql)
            .await
            .context("list source databases")?
            .get(dbname.as_str())
            .context(absent)?;
        pending_dbs.push(PendingDatabase {
            oid,
            name: source_conn.dbname.clone(),
            visible_at,
            bridge_socket: walshadow::bridge::slot_path(
                &bridge_path,
                primary_position * bridge_workers.clamp(1, walshadow::bridge::MAX_BRIDGE_WORKERS),
            ),
        });
    }
    if source_databases.len() > 1 {
        let shadow_dbs = list_databases(
            &open_shadow_sql_client(
                &args.shadow_socket_dir,
                args.shadow_port,
                &args.shadow_user,
                &primary_dbname,
            )
            .await?,
        )
        .await
        .context("list shadow databases")?;
        let source_sql = feed
            .sql_client()
            .await
            .context("source sidecar sql for database list")?;
        let visible_at = walshadow::pg::current_wal_lsn(source_sql)
            .await
            .context("source WAL position for database list")?;
        let source_dbs = list_databases(source_sql)
            .await
            .context("list source databases")?;
        for (index, link) in links.iter_mut().enumerate() {
            if index == primary_position {
                continue;
            }
            let (name, emitter, conninfo) = link_for(index)?;
            let absent = format!("database {name} is configured but not on the source");
            let source_oid = *source_dbs.get(name.as_str()).context(absent)?;
            match shadow_dbs.get(name.as_str()) {
                Some(&oid) if oid == source_oid => {
                    *link = Some(
                        DbLink::connect(DbLinkConfig {
                            name,
                            index,
                            workers: bridge_workers,
                            bridge_path: &bridge_path,
                            shadow_conninfo: &conninfo,
                            budget: connect_budget,
                            emitter,
                        })
                        .await?,
                    );
                }
                Some(&oid) => anyhow::bail!(
                    "database {name} is oid {source_oid} on the source but oid {oid} on the \
                     shadow: it was recreated after the shadow's backup, re-bootstrap"
                ),
                None => {
                    let dir = args.spill_dir.join(format!("db-{source_oid}"));
                    tokio::fs::create_dir_all(&dir)
                        .await
                        .context(format!("create {}", dir.display()))?;
                    tokio::fs::write(dir.join(ATTACH_MARKER), b"")
                        .await
                        .context(format!("mark {} as attaching", dir.display()))?;
                    tracing::warn!(
                        target: "walshadow",
                        database = %name,
                        oid = source_oid,
                        visible_at = %format_pg_lsn(visible_at),
                        "database not in the shadow yet; following it once the shadow replays its creation",
                    );
                    pending_dbs.push(PendingDatabase {
                        oid: source_oid,
                        name: name.clone(),
                        visible_at,
                        bridge_socket: walshadow::bridge::slot_path(
                            &bridge_path,
                            index * bridge_workers.clamp(1, walshadow::bridge::MAX_BRIDGE_WORKERS),
                        ),
                    });
                }
            }
        }
    }
    let db_conns: Vec<DbLink> = links.into_iter().flatten().collect();
    let primary_index = db_conns
        .iter()
        .position(|db| db.name == primary_dbname)
        .expect("primary database linked");
    let shadow_conninfo = socket_conninfo(
        args.shadow_socket_dir
            .to_str()
            .context("shadow-socket-dir not UTF-8")?,
        args.shadow_port,
        &args.shadow_user,
        &primary_dbname,
    );
    let bridge = db_conns[primary_index].bridge.clone();
    let primary_db_oid = db_conns[primary_index].oid;

    // Pre-flight validators run after both source + shadow SQL clients
    // are up so every check has its connection.
    if !args.skip_preflight {
        let source_version_num = feed.server_version_num();
        let source_sql = feed
            .sql_client()
            .await
            .context("source sidecar sql for preflight")?;
        let shadow_sql = open_shadow_sql_client(
            &args.shadow_socket_dir,
            args.shadow_port,
            &args.shadow_user,
            &primary_dbname,
        )
        .await?;
        let mut report = walshadow::preflight::run(walshadow::preflight::Inputs {
            source_version_num,
            source_sql,
            shadow_sql: &shadow_sql,
            slot: source_conn.slot.as_deref(),
            ch_config: ch_config.as_ref(),
        })
        .await
        .context("pre-flight probe")?;
        // Relations of another database resolve over a connection to it
        let others = db_conns.iter().filter(|c| c.name != source_conn.dbname);
        for (conn, cfg) in others.filter_map(|c| Some((c, c.emitter.as_ref()?))) {
            let client = open_source_sql_client(&source_conn, &conn.name)
                .await
                .context(format!("source sql for database {}", conn.name))?;
            report.errors.extend(
                walshadow::preflight::mapped_relations(&client, cfg)
                    .await
                    .context(format!("pre-flight probe for database {}", conn.name))?,
            );
        }
        report
            .into_result()
            .context("pre-flight rejected daemon start")?;
        tracing::info!(target: "walshadow::preflight", "pre-flight passed");
    }

    // Share pump's xid samples with shadow TOAST reads to detect reused IDs
    let xid_ceiling = Arc::new(walshadow::toast::xid_ceiling::XidCeiling::default());
    stream.filter_mut().set_xid_ceiling(xid_ceiling.clone());
    let oracle = Some(Arc::new(
        walshadow::oracle::Oracle::per_database(
            db_conns
                .iter()
                .map(|conn| (conn.oid, conn.bridge.clone()))
                .collect(),
        )
        .with_xid_ceiling(xid_ceiling),
    ));

    // START_REPLICATION runs after sinks are built so archive fallback can
    // advance identical filter and decode paths.
    // Cursor records completed drains atomically; replay WAL to rebuild
    // pending transactions after clearing scratch at startup.
    let mut xact_buf_cfg = XactBufferConfig {
        xact_buffer_max: args.xact_buffer_max,
        ..XactBufferConfig::new(args.scratch_dir())
    };
    if let Some(cfg) = ch_config.as_ref() {
        xact_buf_cfg.drain_batch_rows = cfg.drain_batch_rows;
        xact_buf_cfg.drain_batch_bytes = cfg.drain_batch_bytes;
    }
    // Txn-span registry, shared by pump + decoder + buffer; `Some` only with OTLP on
    let span_registry = (args.otlp_endpoint.is_some()
        || std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok())
    .then(TxnSpanRegistry::new);
    let mut xact_buffer = XactBuffer::new(xact_buf_cfg)
        .context("initialize transaction buffer and scratch directory")?;
    if let Some(reg) = &span_registry {
        xact_buffer = xact_buffer.with_span_registry(reg.clone());
    }
    xact_buffer
        .clear_scratch_dir()
        .context("clear scratch directory")?;
    tracing::info!(
        target: "walshadow",
        spill_dir = %args.spill_dir.display(),
        scratch_dir = %xact_buffer.scratch_dir().display(),
        xact_buffer_max = args.xact_buffer_max,
        "spill and scratch directories ready",
    );
    let xact_buffer = Arc::new(Mutex::new(xact_buffer));

    // Persist handoff before streaming can advance manifest
    if let (Some(end_lsn), Some(resume)) = (bootstrap_end_lsn, bootstrap_resume_lsn) {
        let initial = manifest::Manifest {
            version: manifest::MANIFEST_VERSION,
            // Shadow replayed through end_lsn before handoff, so its bound
            // never cuts below resume ≤ end_lsn
            floor: manifest::FloorInputs {
                resume_safe: Pos::new(resume),
                filter_durable: Pos::new(end_lsn),
                shadow: manifest::ShadowFloor::new(shadow_holds_data, end_lsn, 0),
                ..manifest::FloorInputs::default()
            }
            .floor(),
            source: manifest::SourceIdentity {
                system_id: live_identity.system_id,
                timeline: start_timeline,
                timeline_begin: Pos::new(history.begin_of(start_timeline).unwrap_or(0)),
            },
            wal: manifest::WalBranch {
                stream_timeline: start_timeline,
            },
            lsn: manifest::LsnSet {
                source_received: Pos::new(end_lsn),
                filter_durable: Pos::new(end_lsn),
                shadow_replay: Pos::new(end_lsn),
                drain: Pos::new(resume),
                emitter_ack: Pos::new(resume),
                shadow_flush: Pos::new(end_lsn),
            },
        };
        manifest::write(&args.spill_dir, &initial)
            .await
            .context("write initial resume manifest after bootstrap")?;
    }

    // Descriptor log: durable shape history captured at catalog boundaries,
    // bound to this source + shadow pairing. Sole schema-event source.
    let source_major = supported_major(feed.server_version_num())?;
    stream
        .filter_mut()
        .set_target_dbs(db_conns.iter().map(|conn| conn.oid));
    // Decoder and reorder skip header-routed records; a pending database
    // attaches on any record's block locators
    if pending_dbs.is_empty() {
        stream.filter_mut().enable_header_routing();
    }
    let shadow_toast = ch_config.as_ref().is_some_and(|c| c.toast.mode.is_shadow());
    // Check TOAST availability for opt-in and configured relations
    let mut shadow_toast_held = None;
    if shadow_toast {
        stream
            .filter_mut()
            .load_shadow_rels(shadow_start.data_dir())
            .await?;
        let rels = stream
            .filter()
            .shadow_rels()
            .context("shadow replay eligibility missing after load")?;
        tracing::info!(
            target: "walshadow::toast",
            rels = rels.len(),
            "[toast] mode = shadow: loaded durable replay eligibility",
        );
        shadow_toast_held = Some(rels.held());
    }
    let pending_cfg = ch_config
        .as_ref()
        .map(|c| c.pending_capture)
        .unwrap_or_default();
    let pending_catalog = Arc::new(walshadow::pending::PendingCatalog::default());
    let smgr_markers = stream.filter().smgr_markers();
    let mut stream = stream.start();
    // One log per database, each in its own spill subdirectory; the primary
    // keeps the spill root so a single-database resume reads where it wrote
    let mut desc_logs: Vec<Arc<walshadow::desc_log::DescriptorLog>> =
        Vec::with_capacity(db_conns.len());
    for conn in db_conns.iter() {
        let dir = if conn.name == source_conn.dbname {
            args.spill_dir.clone()
        } else {
            let dir = args.spill_dir.join(format!("db-{}", conn.oid));
            tokio::fs::create_dir_all(&dir)
                .await
                .context(format!("create descriptor log dir {}", dir.display()))?;
            dir
        };
        desc_logs.push(
            open_db_desc_log(DescLogInputs {
                args,
                dir: &dir,
                dbname: &conn.name,
                catalog: &conn.catalog,
                identity: walshadow::desc_log::DescLogIdentity {
                    pg_major: source_major,
                    system_id: ident.system_id.to_string(),
                    // Resume branch, which a crossing moves without moving the
                    // log: the stored header names wherever the log last
                    // rewrote itself, so `lineage` is what places it
                    timeline: start_timeline,
                    db_oid: conn.oid,
                    wal_seg_size: WAL_SEG_SIZE as u32,
                },
                lineage: &lineage,
                // A crash after saving manifest can leave descriptor logs missing
                manifest_present: manifest_at_boot.is_some() && bootstrap_end_lsn.is_none(),
                start_lsn_override,
                raw_start,
                aligned,
            })
            .await?,
        );
    }
    let desc_log = desc_logs[primary_index].clone();
    // Manifest and descriptor logs now preserve resume position
    if bootstrap_end_lsn.is_some() {
        walshadow::bootstrap_marker::BootstrapMarker::complete(shadow_start.data_dir()).await?;
    }
    let all_desc_logs = walshadow::desc_log::DescriptorLogs::new(desc_logs.clone());

    let stash_live = xact_buffer.lock().await.stash_live();
    let mut decoder = BufferingDecoderSink::new(all_desc_logs.clone(), xact_buffer.clone())
        .with_stash_live(stash_live);
    if let Some(schema) = ch_config
        .as_ref()
        .and_then(|c| c.runtime_config_schema.as_deref())
    {
        decoder = decoder.with_config_schema(Arc::from(schema));
    }
    if let Some(reg) = &span_registry {
        decoder = decoder.with_span_registry(reg.clone());
    }
    let decoder_stats_handle = decoder.stats_handle();
    stream
        .filter_mut()
        .set_skip_stats(decoder_stats_handle.clone());
    let skip_attribution_stats = decoder_stats_handle.clone();

    let mut emitter_stats_handle: Option<Arc<EmitterStats>> = None;
    // Seed at resume point so first status write cannot replace persisted ack
    // with zero before WAL re-read catches up
    let emitter_ack = Arc::new(Monotone::<EmitterAck>::new(raw_start.retag()));
    // Persisted resolved floor. Seed with the resolved start: aligned +
    // archive-clamped, the exact position a crash-now restart replays from.
    // Any Dropped queued during the boot re-read of [aligned, raw_start] has
    // commit_lsn ≥ aligned, so its retire holds until a later manifest write
    // moves the floor past it.
    let resume_floor = Arc::new(Monotone::<Floor>::new(aligned));
    // Deferred retires queued before a stop; entries below `aligned` never
    // replay their drop, so the post-spawn flush below is their only route
    // to the wipe. Loaded in metrics-only runs too (inert without a chunk
    // store), preserved for a later CH run over the same spill dir.
    let retires =
        walshadow::toast_retire::RetireLedger::load(&args.spill_dir, live_identity.system_id)
            .await
            .context("load toast retire ledger")?;
    // Pending tables a bootstrap or backup pass left holding undecided rows.
    // Settling needs ClickHouse, so a metrics-only run leaves the ledger for
    // a later CH run over the same spill dir
    let pending_rows = walshadow::visibility_pending::PendingLedger::load(
        &args.spill_dir,
        live_identity.system_id,
    )
    .await
    .context("load pending visibility ledger")?
    .shared();
    // Layered config resolvers (CLI > TOML), one per database. The SIGHUP
    // task re-reads TOML and republishes each
    let mut config_resolvers: Vec<Arc<ConfigResolver>> = Vec::new();
    // Same resolvers keyed by database, for the `database=` labelled series
    let mut resolvers_by_db: HashMap<Oid, Arc<ConfigResolver>> = HashMap::new();
    // COPY backfillers for `initial_load='copy'`, one per database
    let mut copy_backfillers: HashMap<Oid, Arc<walshadow::copy_backfill::CopyBackfiller>> =
        HashMap::new();

    let pcfg = if ch_config.is_some() {
        let mut emitter_cfg = ch_config.clone().expect("[ch] present");
        let addr = format!("{}:{}", emitter_cfg.conn.host, emitter_cfg.conn.port);
        let stats = emitter_stats.clone();
        emitter_stats_handle = Some(stats.clone());
        // One validated resident-payload pool for the pipeline and every
        // concurrent backup pass
        let pipeline_budget =
            walshadow::pipeline::build_budget(&emitter_cfg, emitter_cfg.decoder_pool_size)
                .map_err(anyhow::Error::msg)
                .context("memory budget")?;
        let mut dbs: Vec<Arc<SourceDb>> = Vec::with_capacity(db_conns.len());
        let mut ddl: HashMap<Oid, walshadow::pipeline::DbDdl> = HashMap::new();
        let mut resolvers: Vec<Arc<ConfigResolver>> = Vec::with_capacity(db_conns.len());
        for (i, conn) in db_conns.iter().enumerate() {
            let built = build_source_db(SourceDbInputs {
                args,
                conn,
                cfg: conn.emitter.clone().expect("[ch] present"),
                primary: i == primary_index,
                desc_log: &desc_logs[i],
                spill_dir: if conn.name == source_conn.dbname {
                    args.spill_dir.clone()
                } else {
                    args.spill_dir.join(format!("db-{}", conn.oid))
                },
                source: &source_conn,
                oracle: &oracle,
                history_rx: history_rx.clone(),
                budget: &pipeline_budget,
                stats: &stats,
                source_major,
                raw_start,
                shadow_toast_held: shadow_toast_held.as_ref(),
                system_id: live_identity.system_id,
                pending_rows: &pending_rows,
                tasks: &mut tasks,
            })
            .await
            .context(format!("wire source database {}", conn.name))?;
            if i == primary_index {
                // Seeded + CLI values the initial batcher/inserter run with;
                // they track the watch channel live thereafter
                let rc = built.db.config_rx.as_ref().expect("resolver wired");
                let rc = rc.borrow();
                emitter_cfg.row_budget = rc.row_budget;
                emitter_cfg.byte_budget = rc.byte_budget;
                emitter_cfg.flush_timeout = rc.flush_timeout;
                emitter_cfg.compression = rc.compression;
                emitter_cfg.retry.max_attempts = rc.retry_max_attempts;
            }
            let db_ddl = walshadow::pipeline::DbDdl {
                applicator: built.applicator,
                backfiller: built.backfiller.clone(),
                resolver: built.resolver.clone(),
            };
            ddl.insert(conn.oid, db_ddl);
            copy_backfillers.insert(conn.oid, built.backfiller);
            resolvers.push(built.resolver.clone());
            resolvers_by_db.insert(conn.oid, built.resolver);
            dbs.push(built.db);
        }
        // SIGHUP and `ctl reload` republish every database's scope
        reloader.set_resolvers(resolvers.clone()).await;
        config_resolvers = resolvers;
        let (decoders, inserters) = (
            emitter_cfg.decoder_pool_size,
            emitter_cfg.inserter_pool_size,
        );
        tracing::info!(
            target: "walshadow::pipeline",
            addr = %addr,
            decoders,
            inserters,
            databases = dbs.len(),
            resolvers = bridge.pool_size(),
            "parallel decode+insert pipeline starting",
        );
        PipelineConfig {
            emitter: emitter_cfg,
            decoder_pool_size: decoders,
            inserter_pool_size: inserters,
            dbs: Arc::new(SourceDbs::new(dbs, primary_index)),
            oracle: oracle.clone(),
            ddl,
            tail: TailKind::ClickHouse,
            buffer: xact_buffer.clone(),
            subxact_tracker: Arc::new(Mutex::new(SubxactTracker::new())),
            pending: pending_catalog.clone(),
            stats: stats.clone(),
            span_registry: span_registry.clone(),
            retires,
            pending_rows,
            resume_floor: resume_floor.clone(),
            budget: Some(pipeline_budget),
        }
    } else {
        // Metrics-only (no CH): the identical pipeline with a null tail —
        // zero CH connections, no DDL applicator, no oracle (nothing ships,
        // out-of-matrix values stay stored bytes). The empty mapping routes nothing, so seqs
        // complete at placement and the watermark + slot advance move as in
        // a CH run. Emitter stats stay unexported (`emitter_stats_handle`
        // None), matching the old serial surface.
        // No `[ch]` here, so the CLI layers straight onto the constants.
        let decoders = positive_usize(
            "decoder_pool_size",
            args.decoder_pool_size,
            walshadow::ch_emitter::DEFAULT_DECODER_POOL,
        );
        let inserters = positive_usize(
            "inserter_pool_size",
            args.inserter_pool_size,
            walshadow::ch_emitter::default_inserter_pool(),
        );
        tracing::info!(
            target: "walshadow::pipeline",
            decoders,
            "metrics-only pipeline (null tail) starting",
        );
        let dbs: Vec<Arc<SourceDb>> = db_conns
            .iter()
            .zip(&desc_logs)
            .map(|(conn, desc_log)| Arc::new(metrics_only_db(conn, desc_log)))
            .collect();
        PipelineConfig {
            emitter: EmitterConfig::default(),
            decoder_pool_size: decoders,
            inserter_pool_size: inserters,
            dbs: Arc::new(SourceDbs::new(dbs, primary_index)),
            oracle: None,
            ddl: HashMap::new(),
            tail: TailKind::Null,
            buffer: xact_buffer.clone(),
            subxact_tracker: Arc::new(Mutex::new(SubxactTracker::new())),
            pending: pending_catalog.clone(),
            stats: Arc::new(EmitterStats {
                fail_closed: fail_closed.clone(),
                ..EmitterStats::default()
            }),
            span_registry: span_registry.clone(),
            retires,
            pending_rows: walshadow::visibility_pending::PendingLedger::empty().shared(),
            resume_floor: resume_floor.clone(),
            budget: None,
        }
    };

    let (booting_reorder, pipeline_handle) = pcfg
        .spawn(emitter_ack.clone())
        .await
        .context("spawn decode+insert pipeline")?;
    let ack_probe = pipeline_handle.ack_probe.clone();
    let reorder_sink = booting_reorder
        .boot(
            &args.bootstrap_shadow_data_dir,
            desc_log.active_present_at(raw_start.get()),
            raw_start.get(),
        )
        .await?;
    let mirror_resolver = reorder_sink.planning_resolver();
    let prescan = Arc::new(decoder.prescan(reorder_sink.planning_resolver()));
    let decoder_xact = QueueingRecordSink::spawn(
        DecoderXactPair {
            decoder,
            xact_drain: reorder_sink,
        },
        decoder_batch_size,
        decoder_queue_capacity,
        span_registry.clone(),
    )
    .with_prescan(prescan);
    decoder_xact
        .worker_clock()
        .watch_blocked(emitter_stats.queue_blocked_out_nanos.clone());
    let boundary_gate = CatalogBoundaryGate::new(
        shadow_state.clone(),
        BoundaryGateConfig {
            warn_after: Duration::from_secs(args.catalog_hold_timeout),
            ..BoundaryGateConfig::default()
        },
    )
    .with_local_shadow(db_conns[primary_index].bridge.clone(), metrics.clone());
    let boundary_hold_stats = boundary_gate.stats.clone();
    // One capture per database: a boundary's catalog reads answer over the
    // connection to the database that wrote them
    let mut captures = walshadow::catalog_capture::CaptureSet::default();
    for (conn, log) in db_conns.iter().zip(&desc_logs) {
        captures.insert(
            conn.oid,
            walshadow::catalog_capture::CatalogCapture::new(
                log.clone(),
                conn.catalog.clone(),
                xact_buffer.clone(),
                smgr_markers.clone(),
                pending_catalog.clone(),
                pending_cfg,
            ),
        );
    }
    let capture_stats: HashMap<Oid, Arc<walshadow::catalog_capture::CaptureStats>> =
        captures.stats_handles().collect();
    // Bootstrap's throwaway oracle bridge counts under the database it
    // restored, so its totals carry across handoff rather than reading as a
    // reset once the live bridge takes over
    let metrics_dbs: Vec<DbMetricSources> = db_conns
        .iter()
        .zip(&desc_logs)
        .map(|(conn, log)| DbMetricSources {
            database: conn.name.clone(),
            bridge: [
                Some(conn.bridge.stats.clone()),
                bootstrap_metrics
                    .as_ref()
                    .filter(|_| conn.oid == primary_db_oid)
                    .map(|b| b.bridge.clone()),
            ],
            desc_log: Some(log.clone()),
            capture: capture_stats.get(&conn.oid).cloned(),
            resolver: resolvers_by_db.get(&conn.oid).cloned(),
            backfiller: copy_backfillers.get(&conn.oid).cloned(),
        })
        .collect();
    let decoder_xact = BoundaryHoldSink::new(decoder_xact, boundary_gate)
        .with_capture(captures)
        .with_pending(PendingDatabases::new(
            pending_dbs,
            connect_budget,
            attach.clone(),
        ));
    let mut record_sink = DaemonSinks {
        metrics: MetricsRecordSink::default(),
        decoder_xact,
        decoder_stats: decoder_stats_handle,
        emitter_stats: emitter_stats_handle,
        span_registry,
    };
    // Segment fsync off the hot path: sink writes+renames, the task fsyncs and
    // publishes `durable_lsn`. Seed at the resume point.
    let durable_lsn = Arc::new(Monotone::<FilterDurable>::new(Pos::new(
        stream.dispatched_lsn(),
    )));
    let fsync_fatal = walshadow::pipeline::Fatal::new();
    let (fsync_tx, fsync_rx) = tokio::sync::mpsc::channel::<SegFsync>(SEGMENT_FSYNC_QUEUE);
    // Prior session's writes joined in `run_session`, so no temp is live
    let stale_temps = remove_stale_temps(&args.out_dir)
        .await
        .context("remove stale out-dir write temps")?;
    if stale_temps > 0 {
        tracing::info!(target: "walshadow::retention", count = stale_temps, "removed stale write temps");
    }
    let mut segment_sink =
        DirSegmentSink::with_durability(args.out_dir.clone(), WAL_SEG_SIZE, fsync_tx, io.clone())
            .context("open out-dir")?;
    let out_dir = std::fs::File::open(&args.out_dir).context("open out-dir for syncfs")?;
    let mut fsync_task =
        spawn_segment_fsync(out_dir, fsync_rx, durable_lsn.clone(), fsync_fatal.clone());
    // Descriptor-log GC off the pump task: the pump publishes each persisted
    // floor, the task compacts. Coalesces by construction — a watch holds
    // only the latest floor.
    let gc_fatal = walshadow::pipeline::Fatal::new();
    // Pruner's own cell, not `resume_floor`: dropping it is what tells the gc
    // task the session is done
    let gc_floor = Monotone::<Floor>::default();
    let mut gc_task = spawn_desc_log_gc(desc_log.clone(), gc_floor.watch(), gc_fatal.clone(), io);
    let mut chunk_buf = Vec::with_capacity(64 * 1024);

    // Metrics endpoint + control socket + SIGHUP are process-lifetime (bound in
    // `run`); the session only writes into the shared registry.
    // config_resolvers stay owned here (dropped at session end → mapping
    // refreshers exit); mapping/budget live-reload arrives via the WAL overlay.
    let _ = &config_resolvers;

    // Walreceiver apply LSN (shadow's `GetXLogReplayRecPtr`), joined each pump
    // iteration; feeds the manifest's `shadow_replay`, the shadow floor, the
    // standby-status `apply_lsn` ceiling and the retention cut
    let shadow_replay_lsn = Arc::new(Monotone::<ShadowReplay>::default());
    // Aggregate flush across ShadowStreamSink connections, fed into the
    // cursor for shadow's `START_REPLICATION PHYSICAL` resume on restart.
    let shadow_flush_lsn = Arc::new(Monotone::<ShadowFlush>::default());

    // Retention sweeper drops filtered segments more than `retention_bytes`
    // behind shadow's replay LSN
    if args.retention_bytes > 0 {
        tasks.spawn(
            "retention",
            trim_retention(
                args.out_dir.clone(),
                args.retention_bytes,
                shadow_conninfo.clone(),
                shadow_replay_lsn.clone(),
                DEFAULT_TRIM_INTERVAL,
            ),
        );
    }

    // Block until shadow's walreceiver attaches. `ShadowStreamSink::
    // on_wire_chunk` drops bytes with no connection registered, so a pump
    // racing past `START_REPLICATION`'s LSN before walreceiver arrives
    // leaves an unrecoverable gap: post-conn frames carry LSNs past
    // walreceiver's expected continuity, shadow's apply stalls, the catalog
    // gate times out (pgbench_acceptance / kill_restart failure mode). No
    // attachment fails startup: catalog-boundary holds require a live wire,
    // and archive-only operation can't stop publication at a mid-segment
    // commit (restore_command must never observe unreleased bytes).
    await_walreceiver(
        &shadow_state,
        walsender_addr,
        Duration::from_secs(args.walsender_connect_timeout),
        shutdown,
    )
    .await?;

    let mut source_recovery = SourceRecovery {
        system_id: live_identity.system_id,
        status_interval: Duration::from_secs(args.status_interval),
        backup: archive.as_ref(),
        floor: &resume_floor,
        prefetch: usize::from(args.archive_prefetch),
        backoff: ReconnectBackoff::default(),
        mode_stats: SourceModeStats::default(),
    };
    source_recovery.mode_stats.errors = metrics.snapshot().await.errors;
    let mut timeline_stats = TimelineStats {
        // Off the chain, so a restart after a crossing keeps reporting the fork
        // it resumed across instead of zero
        switch_lsn: history.begin_of(start_timeline).unwrap_or(0),
        ..TimelineStats::default()
    };
    let mut path = SourcePath::Live;
    if let Err(e) = feed
        .start_physical_replication(
            source_conn.slot.as_deref(),
            stream.next_lsn().get(),
            start_timeline,
        )
        .await
    {
        source_recovery
            .mode_stats
            .note_stream_error(stream_error_kind(Some(&e)));
        path = source_recovery
            .attempt(
                Some(e),
                &source_conn,
                &history,
                &stream,
                &mut feed,
                &mut timeline_stats,
            )
            .await?;
    }

    let mut segments_shipped = 0u64;
    let mut config_logged: Option<Instant> = None;
    let mut prev_dispatched = stream.dispatched_lsn();
    let mut rate_estimator = RateEstimator::default();
    // Manifest write cadence. Slot safety doesn't ride on it: advertised
    // flush_lsn is capped at the persisted floor below, so a lagging write
    // only delays slot advance, never overshoots it.
    let cursor_write_interval = Duration::from_secs(args.status_interval);
    let mut last_cursor_write: Option<Instant> = None;
    // A shadow catching up through `restore_command` holds no stream, so its
    // replay has to be polled or the floor freezes
    let mut shadow_replay_poll = walshadow::ticker::Ticker::new(cursor_write_interval);
    // Fast metrics-refresh tick (decoupled from cursor/status): an idle source
    // would otherwise freeze the /metrics snapshot while the pipeline drains.
    let metrics_tick = Duration::from_millis(250);
    // Streaming publish cadence; idle ticks and segment advances always publish
    const PUBLISH_MIN_INTERVAL: Duration = Duration::from_millis(50);
    let mut last_publish = Instant::now();
    // Inflight-stall watchdog: xacts_active > 0 with stalled
    // `emitter_ack_lsn` dumps the parked xids holding the slot. One-shot
    // per stall, re-arms when ack advances.
    let mut last_emitter_ack_observed = Pos::<EmitterAck>::ZERO;
    let mut inflight_stall_since: Option<Instant> = None;
    let mut inflight_stall_logged = false;
    // Pump reads `paused` and the source endpoint live off the resolver watch;
    // when paused it idles (stops consuming source WAL) without tearing
    // anything down, and a moved `[source]` swaps the feed in place.
    // Pause and the source endpoint are cluster-wide, so the primary's
    // resolver is the one the pump watches
    let pump_config_rx = config_resolvers.get(primary_index).map(|r| r.subscribe());
    if let Some(mut rx) = config_resolvers.get(primary_index).map(|r| r.subscribe()) {
        tokio::spawn(async move {
            loop {
                let on = rx.borrow_and_update().skip_attribution;
                skip_attribution_stats
                    .attribute_skips
                    .store(on, std::sync::atomic::Ordering::Relaxed);
                if rx.changed().await.is_err() {
                    break;
                }
            }
        });
    }
    let mut swap = SourceSwap::default();
    // Frontier frozen when the pump observes a pause, so a promotion decision
    // reads one that cannot move under it. Cleared on resume: a value left over
    // from an earlier pause is as misleading as a live one
    let mut pause = PauseState::Running;
    let mut pause_left_stream = false;
    let mut resume_restreams = false;
    let mut ever_unpaused = false;
    // Step 5's answer, refreshed while paused off the endpoint the pump holds
    let mut promotion = PromotionGate::blocked("not_paused");
    let switchover = Switchover {
        system_id: live_identity.system_id,
        out_dir: &args.out_dir,
        shadow_state: &shadow_state,
        backup: archive.as_ref(),
    };
    // The ancestor ended and the descendant has not been adopted yet. Survives
    // iterations so a source error mid-crossing retries the crossing: at the
    // ancestor's switchpoint an ordinary reconnect has nothing to ask for
    let mut crossing = CrossingState::default();
    let mut pump_clock = PumpClock {
        busy_secs: 0.0,
        idle_secs: 0.0,
        hold_secs: 0.0,
        lap: Instant::now(),
        waited_secs: 0.0,
    };
    let mut barrier_logged: Option<Instant> = None;
    let shutdown_reason = loop {
        pump_clock.lap(record_sink.decoder_xact.inner.send_wait_seconds());
        // Switch timelines at a switchpoint. Reconnecting may find a switchpoint
        // missing from `history`; use that connection to fetch updated history.
        if matches!(path, SourcePath::AtSwitchpoint) {
            path = SourcePath::Live;
            crossing.ancestor_ended(false);
        } else if !matches!(path, SourcePath::Live)
            && history.branch_exhausted(stream.timeline(), stream.next_lsn().get())
        {
            path = SourcePath::Live;
            crossing.ancestor_ended(true);
        }
        let paused = pump_config_rx
            .as_ref()
            .map(|rx| rx.borrow().paused)
            .unwrap_or(false);
        if paused && !pause_left_stream && matches!(path, SourcePath::Live) && !crossing.pending() {
            pause_left_stream = true;
            resume_restreams = true;
            if let Err(e) = feed.end_stream().await {
                tracing::warn!(
                    target: "walshadow",
                    error = %format!("{e:#}"),
                    "leaving the source stream for a pause failed; resume redials",
                );
            } else {
                tracing::info!(
                    target: "walshadow",
                    resume_lsn = %stream.next_lsn(),
                    "pause left the source stream so the source can shut down",
                );
            }
        } else if !paused && pause_left_stream {
            pause_left_stream = false;
            if std::mem::take(&mut resume_restreams) {
                let restarted = feed
                    .start_physical_replication(
                        source_conn.slot.as_deref(),
                        stream.next_lsn().get(),
                        stream.timeline(),
                    )
                    .await;
                if let Err(e) = restarted {
                    tracing::warn!(
                        target: "walshadow",
                        error = %format!("{e:#}"),
                        resume_lsn = %stream.next_lsn(),
                        "resuming the source stream failed — redialing",
                    );
                    source_recovery
                        .mode_stats
                        .note_stream_error(stream_error_kind(Some(&e)));
                    path = SourcePath::Redial;
                    source_recovery.backoff.reset();
                } else {
                    tracing::info!(
                        target: "walshadow",
                        resume_lsn = %stream.next_lsn(),
                        "resume re-entered the source stream",
                    );
                }
            }
        }
        // Slot changes require reconnect because START_REPLICATION binds slot
        if let Some(rx) = pump_config_rx.as_ref() {
            let desired = rx.borrow().source.clone();
            if desired != source_conn {
                tracing::info!(
                    target: "walshadow",
                    from = source_conn.endpoint(),
                    to = desired.endpoint(),
                    from_slot = source_conn.slot.as_deref(),
                    to_slot = desired.slot.as_deref(),
                    "source changed — swapping feed",
                );
                source_conn = desired;
                cfg = source_conn.to_pg_config();
                swap.requested();
            }
        }
        // Lost source redials whatever `[source]` names now, so a repoint made
        // during an outage is what the next attempt dials
        if matches!(path, SourcePath::Redial) && source_recovery.backoff.due() {
            path = source_recovery
                .attempt(
                    None,
                    &source_conn,
                    &history,
                    &stream,
                    &mut feed,
                    &mut timeline_stats,
                )
                .await?;
            swap.settled();
        }
        // Swap between chunks, so the resume point is the byte-contiguous
        // `next_lsn` and no WalStream state is rebuilt. Old feed stays up
        // until the new endpoint proves same cluster and branch, and until the
        // named slot answers: a wrong address or a slot the target never got
        // costs a warning, not the stream.
        //
        // Not while a crossing is pending: the stream sits at a switchpoint no
        // branch resumes from, and the crossing dials the live endpoint and slot
        // itself, so a repoint made mid-crossing lands there instead.
        if swap.due(Instant::now()) && !crossing.pending() && !matches!(path, SourcePath::Redial) {
            match resume_source_feed(
                &cfg,
                source_conn.slot.as_deref(),
                stream.next_lsn(),
                stream_branch(&history, live_identity.system_id, &stream),
                resume_floor.get(),
                Duration::from_secs(args.status_interval),
            )
            .await
            {
                Ok((swapped, point)) => {
                    feed = swapped;
                    path = match point {
                        ResumePoint::OnBranch => SourcePath::Live,
                        ResumePoint::AtSwitchpoint => SourcePath::AtSwitchpoint,
                    };
                    resume_restreams = false;
                    swap.settled();
                    if point == ResumePoint::OnBranch {
                        swap.swaps += 1;
                    }
                    tracing::info!(
                        target: "walshadow",
                        endpoint = source_conn.endpoint(),
                        resume_lsn = %stream.next_lsn(),
                        resume_point = ?point,
                        slot = source_conn.slot.as_deref(),
                        "source feed swapped",
                    );
                }
                Err(e) => {
                    swap.failed(swap_reason(&e));
                    timeline_stats.record_reason(swap.blocked_on);
                    tracing::warn!(
                        target: "walshadow",
                        error = %format!("{e:#}"),
                        reason = swap.blocked_on,
                        endpoint = source_conn.endpoint(),
                        "source endpoint swap failed — staying on current feed",
                    );
                }
            }
        }
        // `durable` (fsynced) lags `dispatched`; advertise it as flush/cursor.
        let dispatched = stream.dispatched_lsn();
        let durable = durable_lsn.get();
        let received: Pos<SourceReceived> = Pos::new(feed.last_server_wal_end().max(dispatched));
        // Two frontiers, two questions. `consumed` is where resume asks the
        // promoted target to start; `received` is the source head last heard
        // about, which the target must reach before promotion. Bytes cannot
        // have been consumed without being received, so a source that has not
        // reported a head yet reads as level with the consumed frontier
        if !paused {
            pause = PauseState::Running;
        } else if let PauseState::Running = pause {
            let consumed = stream.next_lsn().get();
            let head = received.get().max(consumed);
            // A pause this process never saw lifted was taken before it
            // booted, so these two numbers replace ones an operator may
            // already hold. Both re-freeze conservatively — consumed drops
            // back to the floor, received re-derives from the live head —
            // but a promotion decision has to be taken from the pair on
            // offer now (architecture/recovery.md)
            let refrozen = !ever_unpaused;
            tracing::info!(
                target: "walshadow",
                pause_consumed_lsn = %format_pg_lsn(consumed),
                pause_received_lsn = %format_pg_lsn(head),
                refrozen,
                "pause observed — frontier frozen",
            );
            pause = PauseState::Paused {
                frontier: (consumed, head),
                refrozen,
                polled_at: None,
            };
        }
        ever_unpaused |= !paused;
        // Step 5 of the protocol, answered off the connection step 4's repoint
        // already moved onto the target: replay, receive, and recovery state
        // beside the frozen frontier they have to reach
        // (architecture/recovery.md)
        if let PauseState::Paused {
            frontier: (_, pause_received),
            polled_at,
            ..
        } = &mut pause
        {
            if polled_at.is_none_or(|t| t.elapsed() >= PROMOTION_POLL) {
                *polled_at = Some(Instant::now());
                promotion = promotion_gate(&mut feed, *pause_received).await;
            }
        } else {
            promotion = PromotionGate::blocked("not_paused");
        }
        let (shadow_agg, shadow_served_tli) = {
            let state = shadow_state.lock().await;
            (state.aggregate(), state.timeline)
        };
        if let Some(apply) = shadow_agg.min_apply_lsn {
            shadow_replay_lsn.join(apply);
        } else if shadow_replay_poll.fire()
            && let Some(Some(lsn)) =
                probe_blocking(&shadow_lifecycle.guard.shadow, |s| s.last_replay_lsn()).await
        {
            shadow_replay_lsn.join(Pos::new(lsn));
        }
        let shadow_replay = shadow_replay_lsn.get();
        if let Some(flush) = shadow_agg.min_flush_lsn {
            shadow_flush_lsn.join(flush);
        }
        let (drain_lsn, resume_safe_lsn) = {
            let mut b = xact_buffer.lock().await;
            let ea = emitter_ack.get();
            let drain_lsn = b.stats().drain_lsn;
            // Keep every undurable transaction reachable after restart
            // Read acknowledgment first so no transaction escapes floor
            (drain_lsn, b.resume_safe_lsn(ea))
        };
        let shadow_floor =
            manifest::ShadowFloor::new(shadow_toast, shadow_replay.get(), shadow_replay_seed);
        let cur = resume_manifest(
            &history,
            &live_identity,
            resume_floor.get(),
            shadow_floor,
            stream.timeline(),
            manifest::LsnSet {
                source_received: received,
                filter_durable: durable,
                shadow_replay,
                drain: drain_lsn,
                emitter_ack: resume_safe_lsn,
                shadow_flush: shadow_flush_lsn.get(),
            },
        );
        if last_cursor_write.is_none_or(|t| t.elapsed() >= cursor_write_interval) {
            mirror_resolver
                .flush_mirror()
                .await
                .context("flush TOAST mirror before publishing the floor")?;
            let persisted = manifest::write(&args.spill_dir, &cur)
                .await
                .context("write resume manifest")?;
            #[cfg(feature = "test-support")]
            crash_test::hit(crash_test::Point::AfterManifest, cur.floor.get());
            last_cursor_write = Some(Instant::now());
            resume_floor.publish(persisted);
            // Descriptor log prunes against the same floor, off this task: a
            // compaction rewrites the whole ckpt inline and would stall WAL
            // consumption past the source's wal_sender_timeout
            gc_floor.publish(persisted);
        }
        // flush caps physical slot's restart_lsn.
        // Manifest writes are cadence-gated above while keepalive replies inside
        // next_event can send this status at any time.
        let status = StandbyStatus::bounded(
            received,
            resume_floor.get(),
            resume_safe_lsn,
            shadow_replay,
            shadow_floor,
        );
        let dispatched_before = stream.dispatched_lsn();
        // Set inside the select arm, acted on once the chunk borrow is released
        let mut ancestor_ended = false;
        let archived_bytes;
        let mut archived_segment = false;
        let waiting = Instant::now();
        let chunk = tokio::select! {
            biased;
            () = shutdown.cancelled() => break "signal",
            Some(err) = tasks.exited() => return Err(err),
            res = &mut fsync_task => return Err(task_stopped("segment fsync", res, &fsync_fatal)),
            res = &mut gc_task => return Err(task_stopped("descriptor log gc", res, &gc_fatal)),
            // Surfaced by the check after the crossing step
            _ = pipeline_handle.fatal.wait() => None,
            // Idle tick so metrics/cursor keep tracking, and so a `paused` flip
            // is picked up promptly.
            _ = tokio::time::sleep(metrics_tick) => None,
            // Paused: stop consuming source WAL (idle); resume re-enables this
            // arm and the pump continues from the same LSN. A pending crossing
            // also parks it — that connection is out of COPY until the
            // descendant is requested.
            result = async { path.archive()?.next().await },
                if matches!(path, SourcePath::Archive(_)) && !paused && !crossing.pending() => {
                match result {
                    Some(Ok((start_lsn, bytes))) => {
                        source_recovery.backoff.reset();
                        anyhow::ensure!(start_lsn == stream.next_lsn().get(), "archive WAL discontinuity");
                        archived_bytes = bytes;
                        archived_segment = true;
                        Some(walshadow::source_feed::WalChunk {
                            start_lsn,
                            server_wal_end: start_lsn + archived_bytes.len() as u64,
                            data: &archived_bytes,
                        })
                    }
                    ended => {
                        let reason = ended.and_then(Result::err).map_or_else(
                            || "archive reader stopped".to_string(),
                            |e| format!("{e:#}"),
                        );
                        tracing::info!(target: "walshadow", reason, "archive ended, reconnecting source");
                        path = SourcePath::Redial;
                        None
                    }
                }
            },
            res = feed.next_event(status, &mut chunk_buf),
                if matches!(path, SourcePath::Live) && !paused && !crossing.pending() => match res {
                Ok(SourceEvent::Wal(c)) => Some(c),
                Ok(SourceEvent::TimelineEnd) => {
                    ancestor_ended = true;
                    None
                }
                // Dropped where the chain says the branch ends: nothing is
                // resumable there, so this is the crossing arriving as a socket
                // close rather than as a next-timeline result
                Ok(SourceEvent::Shutdown) | Err(_)
                if history.branch_exhausted(stream.timeline(), stream.next_lsn().get()) =>
            {
                    tracing::info!(
                        target: "walshadow",
                        switch_lsn = %stream.next_lsn(),
                        finished_timeline = stream.timeline(),
                        "source stream ended where the branch does — crossing",
                    );
                    crossing.ancestor_ended(true);
                    None
                }
                // The source stopped, this consumer did not: reconnect, which
                // is also how a switchover's demoted primary hands over
                res => {
                    let err = match res {
                        Err(e) => {
                            source_recovery.mode_stats.note_stream_error(stream_error_kind(Some(&e)));
                            tracing::warn!(
                                target: "walshadow",
                                error = %e,
                                resume_lsn = %stream.next_lsn(),
                                "source stream error — recovering",
                            );
                            e
                        }
                        _ => {
                            source_recovery.mode_stats.note_stream_error(stream_error_kind(None));
                            tracing::info!(
                                target: "walshadow",
                                resume_lsn = %stream.next_lsn(),
                                "source shut down its walsender — reconnecting",
                            );
                            anyhow::anyhow!("source walsender exited")
                        }
                    };
                    path = source_recovery
                        .attempt(
                            Some(err),
                            &source_conn,
                            &history,
                            &stream,
                            &mut feed,
                            &mut timeline_stats,
                        )
                        .await?;
                    // Recovery dials the live endpoint, so a queued swap is done
                    swap.settled();
                    None
                }
            },
        };
        pump_clock.idle_secs += waiting.elapsed().as_secs_f64();
        let idle = chunk.is_none();
        let server_end = chunk
            .as_ref()
            .map(|c| c.server_wal_end)
            .unwrap_or(received.get());
        if let Some(chunk) = chunk {
            let replay_started = Instant::now();
            let hold_before = boundary_hold_stats.hold_seconds_total();
            let push = stream.push(
                chunk.start_lsn,
                chunk.data,
                &mut record_sink,
                &mut segment_sink,
            );
            tokio::pin!(push);
            loop {
                let status_due = feed.parked_status_due();
                tokio::select! {
                    biased;
                    result = &mut push => {
                        result?;
                        break;
                    }
                    () = async {
                        shutdown.cancelled().await;
                        let _ = boundary_hold_stats.parked.subscribe().wait_for(|&parked| parked).await;
                    } => {
                        // Shadow may never replay the boundary, and a partial push
                        // cannot publish a new resume cursor
                        tracing::info!(target: "walshadow", "stopping during catalog boundary hold");
                        return Ok(());
                    }
                    Some(err) = tasks.exited() => return Err(err),
                    res = &mut fsync_task => return Err(task_stopped("segment fsync", res, &fsync_fatal)),
                    res = &mut gc_task => return Err(task_stopped("descriptor log gc", res, &gc_fatal)),
                    msg = pipeline_handle.fatal.wait() => anyhow::bail!("decode+insert pipeline failed: {msg}"),
                    () = async { tokio::time::sleep_until(status_due.unwrap()).await },
                        if matches!(path, SourcePath::Live) && status_due.is_some() => {
                        feed.send_parked_status(status).await;
                    }
                }
            }
            let held = boundary_hold_stats.hold_seconds_total() - hold_before;
            pump_clock.hold_secs += held;
            if archived_segment {
                metrics
                    .update(|snap| {
                        snap.archive_wal_segments_total += 1;
                        snap.archive_replay_seconds_total += replay_started.elapsed().as_secs_f64();
                    })
                    .await;
            }
        }
        source_recovery
            .mode_stats
            .observe(path.mode(paused, crossing.pending()));
        metrics
            .update(|snap| {
                snap.source_mode = source_recovery.mode_stats.gauge();
                snap.errors = source_recovery.mode_stats.errors;
                merge_worker_seconds(
                    &mut snap.worker_seconds,
                    pipeline_worker_seconds(
                        &pump_clock,
                        record_sink.decoder_xact.inner.send_wait_seconds(),
                        record_sink.decoder_xact.inner.worker_clock().seconds(),
                        record_sink.emitter_stats.as_deref(),
                    ),
                );
                snap.archive_restore_active = 0;
                if let SourcePath::Archive(reader) = &path {
                    snap.archive_restore_active = 1;
                    snap.archive_fetch_seconds_total +=
                        reader.fetch_nanos.swap(0, Ordering::Relaxed) as f64 / 1e9;
                    snap.archive_wait_seconds_total +=
                        reader.wait_nanos.swap(0, Ordering::Relaxed) as f64 / 1e9;
                }
            })
            .await;
        if ancestor_ended {
            // Answer the backend's CopyDone now, leaving the connection in
            // simple-query mode: that is the state the crossing reads history
            // from, and the state a retry can rebuild by reconnecting
            let ended = feed.end_stream().await;
            if let Err(e) = &ended {
                tracing::warn!(
                    target: "walshadow",
                    error = %format!("{e:#}"),
                    "ending the historic stream failed — reconnecting to cross",
                );
            }
            crossing.ancestor_ended(ended.is_err());
        }
        // Nothing left to stream on the ancestor at its own switchpoint, so
        // only the crossing moves the stream forward. Attempts pace themselves
        // and leave the rest of the loop publishing meanwhile
        // A pause takes the crossing decision back from the pump, so it also
        // clears a wedge: the operator fixes what the refusal named, then
        // resumes and the proof runs again from the untouched ancestor
        if paused && let Some(wedge) = crossing.unpark() {
            tracing::info!(
                target: "walshadow",
                reason = wedge.reason,
                "pause clears the parked crossing — resume re-proves the fork",
            );
        }
        let crossing_due = !paused && crossing.due(Instant::now());
        if crossing_due && crossing.awaiting_connection() {
            match SourceFeed::connect(&cfg).await {
                Ok(fresh) => {
                    feed = fresh.with_status_interval(Duration::from_secs(args.status_interval));
                    crossing.connected();
                }
                Err(e) => {
                    tracing::warn!(
                        target: "walshadow",
                        error = %format!("{e:#}"),
                        endpoint = source_conn.endpoint(),
                        "cannot reach the source to cross the fork — retrying",
                    );
                    crossing.retry_at(Instant::now() + SOURCE_SWAP_RETRY);
                }
            }
        }
        if crossing_due && !crossing.awaiting_connection() && crossing.fork().is_none() {
            match switchover
                .probe(
                    &mut feed,
                    &stream,
                    history.begin_of(stream.timeline()).unwrap_or(0),
                    &mut timeline_stats,
                )
                .await
            {
                Ok(probed) => {
                    tracing::info!(
                        target: "walshadow",
                        finished_timeline = probed.finished_tli,
                        next_timeline = probed.next_tli,
                        live_timeline = probed.live_tli,
                        switch_lsn = %format_pg_lsn(probed.switch_lsn),
                        "source fork proved — draining the pipeline to it",
                    );
                    crossing.proved(probed);
                }
                Err(e) if e.retryable() => {
                    tracing::warn!(
                        target: "walshadow",
                        error = %format!("{e:#}"),
                        reason = e.reason(),
                        "proving the source fork failed — retrying",
                    );
                    crossing.retry_from_source(Instant::now() + SOURCE_SWAP_RETRY);
                }
                Err(e) => crossing.park(e, stream.next_lsn().get(), None),
            }
        }
        if crossing_due
            && !crossing.awaiting_connection()
            && let Some(probed) = crossing.fork().cloned()
        {
            // Read before the guards: with the source arms parked, an empty
            // queue stays empty, so the decoder's view they read is final
            record_sink
                .decoder_xact
                .flush()
                .await
                .context("flush queueing decoder sink at the fork")?;
            let pump_in_flight = record_sink.decoder_xact.in_flight();
            let (guards, resume_safe) = {
                let mut b = xact_buffer.lock().await;
                let ea = emitter_ack.get();
                let resume_safe = b.resume_safe_lsn(ea);
                let stats = b.stats();
                (
                    ForkGuards {
                        drain_lsn: stats.drain_lsn,
                        open_xacts: stats.xacts_active as usize,
                    },
                    resume_safe,
                )
            };
            // Barrier: every consumer past the position about to be committed,
            // so a restart from it loses nothing. The loop keeps publishing
            // meanwhile, so a wait reads as a wait rather than a stall, and the
            // source has stopped producing so nothing queues up behind it
            let open = match (walshadow::transition::ForkBarrier {
                pump_in_flight,
                resume_safe_lsn: resume_safe,
                shadow_apply_lsn: (shadow_replay.get() != 0).then_some(shadow_replay),
                filter_durable: durable,
                floor: resume_floor.get(),
            })
            .open(Pos::new(probed.switch_lsn), WAL_SEG_SIZE)
            {
                Ok(open) => Some(open),
                Err(wait) => {
                    // Prod the walreceiver: non-forced replies fire only on flush
                    // progress, and the ancestor's tail may be the last thing left
                    shadow_state.lock().await.request_status();
                    if barrier_logged.is_none_or(|t| t.elapsed() >= BARRIER_LOG_INTERVAL) {
                        tracing::info!(
                            target: "walshadow",
                            switch_lsn = %format_pg_lsn(probed.switch_lsn),
                            waiting_on = wait.label(),
                            "fork barrier: {wait}",
                        );
                        barrier_logged = Some(Instant::now());
                    }
                    None
                }
            };
            if let Some(open) = open {
                barrier_logged = None;
                let commit = async |resume: walshadow::transition::ForkResume| {
                    commit_fork_resume(
                        &args.spill_dir,
                        &live_identity,
                        resume,
                        manifest::LsnSet {
                            // Fork cannot precede last observed source head
                            source_received: received.max(resume.switch_lsn().retag()),
                            filter_durable: durable,
                            shadow_replay,
                            drain: guards.drain_lsn,
                            emitter_ack: resume_safe,
                            shadow_flush: shadow_flush_lsn.get(),
                        },
                        &resume_floor,
                        &gc_floor,
                    )
                    .await
                };
                match switchover
                    .cross(
                        &mut feed,
                        source_conn.slot.as_deref(),
                        &mut stream,
                        &mut record_sink,
                        &mut segment_sink,
                        status,
                        guards,
                        &probed,
                        open,
                        commit,
                        &mut timeline_stats,
                    )
                    .await
                {
                    Ok(crossed) => {
                        tracing::info!(
                            target: "walshadow",
                            system_id = live_identity.system_id,
                            finished_timeline = crossed.finished_tli,
                            next_timeline = crossed.next_tli,
                            live_timeline = crossed.live_tli,
                            switch_lsn = %format_pg_lsn(crossed.switch_lsn),
                            resume_lsn = %stream.next_lsn(),
                            floor_lsn = %resume_floor.get(),
                            drain_lsn = %guards.drain_lsn,
                            prefix_bytes_verified = crossed.prefix_bytes,
                            slot = source_conn.slot.as_deref(),
                            "crossed source timeline",
                        );
                        if crossed.prefix_origin == PrefixOrigin::Archive {
                            source_recovery.backoff.reset();
                            path = SourcePath::Redial;
                        }
                        history = crossed.history;
                        history_tx.send_replace(Arc::new(history.clone()));
                        crossing.committed();
                        swap.settled();
                    }
                    // Lineage, prefix, and publication proofs need an operator; a
                    // source or storage error is worth another attempt. Every
                    // retryable failure lands before the commit, so the retry
                    // starts from the same proof against an untouched ancestor
                    Err(e) if e.retryable() => {
                        tracing::warn!(
                            target: "walshadow",
                            error = %format!("{e:#}"),
                            reason = e.reason(),
                            stream_timeline = stream.timeline(),
                            "timeline crossing failed — retrying",
                        );
                        crossing.retry_from_source(Instant::now() + SOURCE_SWAP_RETRY);
                    }
                    Err(e) => crossing.park(e, stream.next_lsn().get(), Some(probed.switch_lsn)),
                }
            }
        }
        // Flush pump-side accumulator so partial batches don't strand
        // commits in `decoder_xact.buf` when source goes idle (kill-restart
        // post-catchup quiescence).
        record_sink
            .decoder_xact
            .flush()
            .await
            .context("flush queueing decoder sink")?;
        // Surface a pipeline-stage failure as a clean daemon exit with the
        // root cause rather than a silently pinned watermark.
        if let Some(msg) = pipeline_handle.fatal.message() {
            anyhow::bail!("decode+insert pipeline failed: {msg}");
        }
        let now_dispatched = stream.dispatched_lsn();
        let advanced = now_dispatched != prev_dispatched;
        // Streaming iterates per walsender message; publishing each time reads
        // /proc and takes the xact buffer lock from the worker
        if !idle && !advanced && last_publish.elapsed() < PUBLISH_MIN_INTERVAL {
            continue;
        }
        last_publish = Instant::now();
        // Re-read rather than reuse the top-of-iteration pair: a crossing commits
        // a new floor and branch mid-iteration, and this is what an operator
        // watches to know the crossing is durable
        let published_floor = cur.floor.max(resume_floor.get());
        let published_branch = history.floor_branch(
            published_floor.get(),
            live_identity.timeline,
            stream.timeline(),
            WAL_SEG_SIZE,
        );
        let (xact_stats, drain_resident, xact_line) = {
            let b = xact_buffer.lock().await;
            let stats = b.stats().clone();
            let line = stats.summary();
            let resident = DrainResident::from_buffer(&b);
            (stats, resident, line)
        };
        let oracle_line = oracle
            .as_ref()
            .map(|o| o.stats.summary())
            .unwrap_or_default();
        let oracle_stats = oracle.as_ref().map(|o| o.stats.as_ref());
        // Per database, since each dials its own sockets and one can be down
        // while the rest answer
        let bridge_line = db_conns
            .iter()
            .map(|conn| format!("{}={}", conn.name, conn.bridge.stats.summary()))
            .collect::<Vec<_>>()
            .join(" ");
        let decoder_stats: &walshadow::decoder_sink::DecoderStats = &record_sink.decoder_stats;
        let emitter_stats: Option<&walshadow::ch_emitter::EmitterStats> =
            record_sink.emitter_stats.as_deref();
        let shadow_apply_lsn = shadow_replay.get();
        rate_estimator.observe(Instant::now(), received.get());
        let apply_lag = shadow_apply_lag(received.get(), shadow_apply_lsn, &rate_estimator);
        // Post-worker snapshots so the metric reflects what the worker
        // drained, not the top-of-iteration values.
        let emitter_ack_for_metric = emitter_ack.get();
        let drain_for_metric = xact_stats.drain_lsn;
        let ack_commit_ts = ack_probe.borrow().ack_commit_ts;
        let streaming =
            path.mode(paused, crossing.pending()) == walshadow::metrics::SourceMode::Streaming;
        let source_wal_end = streaming.then(|| feed.last_server_wal_end());
        if config_logged.is_none_or(|t| t.elapsed() >= CONFIG_LOG_INTERVAL) {
            config_logged = Some(Instant::now());
            for resolver in &config_resolvers {
                log_replication_config(
                    &resolver.subscribe().borrow(),
                    &source_conn.endpoint(),
                    feed.server_version_num(),
                    &ident.system_id,
                );
            }
        }
        populate_metrics(
            &metrics,
            received,
            source_wal_end,
            Pos::new(now_dispatched),
            shadow_replay,
            drain_for_metric,
            emitter_ack_for_metric,
            ack_commit_ts,
            stream.filter().stats().last_commit_ts,
            &record_sink.metrics,
            record_sink.decoder_xact.in_flight(),
            record_sink.decoder_xact.processed(),
            &xact_stats,
            drain_resident,
            Some(&pipeline_handle.budget),
            decoder_stats,
            &swap,
            TimelineView {
                source_system_id: live_identity.system_id,
                source_timeline: stream.timeline(),
                floor_timeline: published_branch,
                shadow_served_timeline: shadow_served_tli,
                shadow_replay_timeline: shadow_agg.replay_timeline.unwrap_or(0),
                floor_lsn: published_floor,
                stats: timeline_stats,
                pause_frontier: pause.frontier(),
                pause_refrozen: pause.refrozen(),
                wedge: crossing.wedge().cloned(),
                promotion,
            },
            ShadowMetricsView {
                apply_lag,
                active_connections: shadow_agg.active_connections as u64,
                socket_events: shadow_agg.socket_events,
                backfill_bytes_total: shadow_agg.backfill_bytes_total,
                send_queue_bytes: shadow_agg.send_queue_bytes,
            },
            &boundary_hold_stats,
            metrics_dbs.iter().map(DbMetricSources::series).collect(),
            StageCounters {
                emitter: emitter_stats,
                db_name: &|oid| {
                    db_conns
                        .iter()
                        .find(|c| c.oid == oid)
                        .map(|c| c.name.clone())
                        .unwrap_or_default()
                },
                oracle: [oracle_stats, bootstrap_metrics.as_ref().map(|b| &*b.oracle)],
                bootstrap: bootstrap_metrics.as_ref().map(|b| &b.progress),
                bootstrap_attempt: 0,
                uptime_secs: start_instant.elapsed().as_secs(),
            },
        )
        .await;
        if advanced {
            let new_segs = (now_dispatched - prev_dispatched) / WAL_SEG_SIZE;
            segments_shipped += new_segs;
            prev_dispatched = now_dispatched;
            let ahead = server_end.saturating_sub(dispatched_before);
            let filter = stream.filter();
            let filter_stats = filter.stats();
            let tracker_stats = filter.tracker().stats();
            tracing::info!(
                target: "walshadow",
                segments_shipped,
                last_lsn = format_pg_lsn(now_dispatched).to_string(),
                shadow_apply = format_pg_lsn(shadow_apply_lsn).to_string(),
                source_ahead_bytes = ahead,
                metrics = %record_sink.metrics.summary(),
                kept = filter_stats.kept,
                dropped = filter_stats.dropped,
                relmap_updates = tracker_stats.relmap_updates,
                pg_class_undecoded = tracker_stats.pg_class_writes_undecoded,
                pg_class_oid_in_prefix = tracker_stats.pg_class_writes_oid_in_prefix,
                decoder = %decoder_stats.summary(),
                xact_buffer = %xact_line,
                oracle = %oracle_line,
                bridge = %bridge_line,
                "status",
            );
            if args.max_segments != 0 && segments_shipped >= args.max_segments {
                break "max-segments";
            }
        }
        // Re-arm on ack move; else after 5s of stall with parked xacts dump
        // the xids once. Runs independent of `advanced` so a fully-quiescent
        // pump still surfaces who's holding the slot.
        if emitter_ack_for_metric != last_emitter_ack_observed {
            last_emitter_ack_observed = emitter_ack_for_metric;
            inflight_stall_since = None;
            inflight_stall_logged = false;
        }
        // Ack can pin after transaction leaves buffer
        let ack_snap = *ack_probe.borrow();
        if xact_stats.xacts_active > 0 || !ack_snap.all_done() || ack_snap.wedged != 0 {
            let since = inflight_stall_since.get_or_insert(Instant::now());
            if !inflight_stall_logged && since.elapsed() >= Duration::from_secs(5) {
                let snap = xact_buffer.lock().await.inflight_snapshot();
                let summary: String = snap
                    .iter()
                    .map(|e| {
                        format!(
                            "xid={} lsn={}..{} heap={} chunk={} bytes={} spill={} cat={} rels=[{}]",
                            e.xid,
                            format_pg_lsn(e.first_lsn),
                            format_pg_lsn(e.last_lsn),
                            e.heap_count,
                            e.chunk_count,
                            e.in_mem_bytes,
                            if e.spilled { "y" } else { "n" },
                            e.catalog_events,
                            e.rels,
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(" | ");
                tracing::warn!(
                    target: "walshadow",
                    xacts_active = xact_stats.xacts_active,
                    emitter_ack_lsn = %emitter_ack_for_metric,
                    drain_lsn = %xact_stats.drain_lsn,
                    source_received = %received,
                    filter_dispatched = format_pg_lsn(now_dispatched).to_string(),
                    inflight = %summary,
                    ack = ?ack_snap,
                    waiting_on = ack_snap.stall_reason().unwrap_or("buffered xacts"),
                    "emitter ack pinned",
                );
                inflight_stall_logged = true;
            }
        } else {
            inflight_stall_since = None;
            inflight_stall_logged = false;
        }
    };
    drop(path);
    tracing::info!(
        target: "walshadow",
        reason = shutdown_reason,
        out_dir = %args.out_dir.display(),
        "stopping",
    );
    let final_timeline = stream.timeline();
    let final_received = stream.next_lsn().get();
    // Close the sink (finishes its write, closes the fsync queue) and drain
    // the fsync task so sealed segments are durable
    segment_sink
        .close()
        .await
        .context("write final filtered segment")?;
    task_finished("segment fsync", fsync_task.await, &fsync_fatal)?;
    // Close the floor channel and join: nothing else may own desc_log.ckpt
    // after the session returns
    drop(gc_floor);
    task_finished("descriptor log gc", gc_task.await, &gc_fatal)?;
    // Drain queueing worker so enqueued-but-undispatched records run
    // through decoder + xact_drain before exit; surfaces worker-parked errors.
    let DaemonSinks { decoder_xact, .. } = record_sink;
    decoder_xact
        .close()
        .await
        .context("drain queueing decoder sink on shutdown")?;
    // Worker close dropped the reorder sink. Drain rest in order
    // (batcher force-flush → inserters to
    // EndOfStream → ack collector) so no rows are lost + final watermark durable.
    pipeline_handle
        .join()
        .await
        .map_err(|m| anyhow::anyhow!("decode+insert pipeline drain failed: {m}"))?;
    let (drain, resume_safe) = {
        let mut b = xact_buffer.lock().await;
        let ea = emitter_ack.get();
        let drain = b.stats().drain_lsn;
        (drain, b.resume_safe_lsn(ea))
    };
    let shadow_replay = shadow_replay_lsn.get();
    let durable = durable_lsn.get();
    manifest::write(
        &args.spill_dir,
        &resume_manifest(
            &history,
            &live_identity,
            resume_floor.get(),
            manifest::ShadowFloor::new(shadow_toast, shadow_replay.get(), shadow_replay_seed),
            final_timeline,
            manifest::LsnSet {
                source_received: Pos::new(final_received),
                filter_durable: durable,
                shadow_replay,
                drain,
                emitter_ack: resume_safe,
                shadow_flush: shadow_flush_lsn.get(),
            },
        ),
    )
    .await
    .context("write shutdown resume manifest")?;
    shadow_lifecycle.shutdown().await;
    tasks.shutdown().await
}

/// Source PG major, refusing any whose commit-record sinval layout is unaudited
fn supported_major(server_version_num: i32) -> Result<u32> {
    let major = (server_version_num / 10000) as u32;
    anyhow::ensure!(
        (16..=19).contains(&major),
        "source PG major {major} unsupported (commit-record sinval layout audited for 16-19)",
    );
    Ok(major)
}

/// Session tasks meant to run until shutdown; any exit before it is fatal
#[derive(Default)]
pub(crate) struct SessionTasks {
    set: tokio::task::JoinSet<()>,
    names: foldhash::HashMap<tokio::task::Id, &'static str>,
}

impl SessionTasks {
    pub(crate) fn spawn(
        &mut self,
        name: &'static str,
        task: impl Future<Output = ()> + Send + 'static,
    ) {
        let id = self.set.spawn(task).id();
        self.names.insert(id, name);
    }

    /// Supervise a task spawned elsewhere, aborting it with the set
    pub(crate) fn adopt(&mut self, name: &'static str, handle: tokio::task::JoinHandle<()>) {
        let handle = tokio_util::task::AbortOnDropHandle::new(handle);
        self.spawn(name, async move {
            if let Err(e) = handle.await
                && e.is_panic()
            {
                std::panic::resume_unwind(e.into_panic());
            }
        });
    }

    fn name(&self, id: tokio::task::Id) -> &'static str {
        self.names.get(&id).copied().unwrap_or("session")
    }

    /// First task to stop, as the error naming it. `None` once the set is empty
    async fn exited(&mut self) -> Option<anyhow::Error> {
        let res = self.set.join_next_with_id().await?;
        Some(match res {
            Ok((id, ())) => anyhow::anyhow!("{} task exited", self.name(id)),
            Err(e) => anyhow::anyhow!("{} task failed: {e}", self.name(e.id())),
        })
    }

    /// Abort what still runs, surfacing a task that panicked
    async fn shutdown(mut self) -> Result<()> {
        self.set.abort_all();
        while let Some(res) = self.set.join_next_with_id().await {
            if let Err(e) = res
                && e.is_panic()
            {
                anyhow::bail!("{} task failed: {e}", self.name(e.id()));
            }
        }
        Ok(())
    }
}

fn log_replication_config(
    cfg: &walshadow::config::ResolvedConfig,
    source_endpoint: &str,
    source_version_num: i32,
    system_id: &dyn std::fmt::Display,
) {
    tracing::info!(
        target: "walshadow::config",
        event = "shadow config",
        source_endpoint,
        source_version_num,
        system_id = %system_id,
        source_dbname = %cfg.source.dbname,
        tables = cfg.tables.len(),
        namespaces = cfg.namespaces.len(),
        paused = cfg.paused,
        row_budget = cfg.row_budget,
        byte_budget = cfg.byte_budget,
    );
    let mut tables: Vec<_> = cfg.tables.iter().collect();
    tables.sort_by(|a, b| a.0.cmp(b.0));
    for (rel, mapping) in tables {
        tracing::info!(
            target: "walshadow::config",
            event = "table config",
            source_table = %rel,
            target_database = %mapping.target.database,
            target_table = %mapping.target.table,
            target_instance = mapping.target.instance.as_deref().unwrap_or(""),
            columns = mapping.columns.len(),
            tees = mapping.tees.len(),
        );
    }
}

/// Error for a task that stopped before its shutdown, preferring the fatal
/// it set on the way out
pub(crate) fn task_stopped(
    name: &str,
    res: Result<(), tokio::task::JoinError>,
    fatal: &walshadow::pipeline::Fatal,
) -> anyhow::Error {
    match (fatal.message(), res) {
        (Some(msg), _) => anyhow::anyhow!("{name} failed: {msg}"),
        (None, Ok(())) => anyhow::anyhow!("{name} task exited"),
        (None, Err(e)) => anyhow::anyhow!("{name} task failed: {e}"),
    }
}

/// Join of a task told to finish, failing on a panic or a fatal it set
fn task_finished(
    name: &str,
    res: Result<(), tokio::task::JoinError>,
    fatal: &walshadow::pipeline::Fatal,
) -> Result<()> {
    if res.is_err() || fatal.is_set() {
        return Err(task_stopped(name, res, fatal));
    }
    Ok(())
}

fn substitute_database(databases: &[String], absent: &str, replacement: &str) -> Vec<String> {
    let mut seen = foldhash::HashSet::default();
    databases
        .iter()
        .map(|db| if db == absent { replacement } else { db })
        .filter(|db| seen.insert(*db))
        .map(str::to_owned)
        .collect()
}

async fn stand_in_primary(shadow: &Arc<walshadow::shadow::Shadow>, absent: &str) -> Option<String> {
    if probe_blocking(shadow, |s| s.has_database()).await != Some(false) {
        return None;
    }
    let want = absent.to_owned();
    probe_blocking(shadow, move |s| {
        s.probe_one(
            "SELECT datname FROM pg_database WHERE datallowconn AND NOT datistemplate \
             ORDER BY datname = 'postgres' DESC, oid LIMIT 1",
        )
    })
    .await
    .filter(|name| !name.is_empty() && *name != want)
}

#[cfg(test)]
mod tests {
    use super::*;
    use walshadow::ch_emitter::InstanceConfig;

    #[test]
    fn stand_in_reuses_an_already_followed_database() {
        for databases in [["late", "postgres", "early"], ["postgres", "late", "early"]] {
            assert_eq!(
                substitute_database(&databases.map(String::from), "late", "postgres"),
                ["postgres", "early"]
            );
        }
        assert_eq!(
            substitute_database(&["early".into(), "late".into()], "late", "postgres"),
            ["early", "postgres"]
        );
    }

    #[tokio::test]
    async fn boot_redials_clickhouse_that_drops_connections() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let cfg = EmitterConfig {
            conn: InstanceConfig {
                host: "127.0.0.1".into(),
                port: listener.local_addr().unwrap().port(),
                ..Default::default()
            },
            ..EmitterConfig::default()
        };
        let accepts = async {
            for _ in 0..2 {
                drop(listener.accept().await.unwrap());
            }
        };
        tokio::select! {
            _ = ensure_boot_database_waiting(&cfg) => panic!("boot proceeded without ClickHouse"),
            () = accepts => {}
        }
    }

    #[tokio::test]
    async fn session_tasks_name_the_task_that_stopped() {
        let mut tasks = SessionTasks::default();
        tasks.spawn("idle", std::future::pending());
        tasks.spawn("quits", async {});
        let err = tasks.exited().await.unwrap().to_string();
        assert!(err.contains("quits task exited"), "{err}");
        tasks.spawn("panics", async { panic!("boom") });
        let err = tasks.exited().await.unwrap().to_string();
        assert!(
            err.contains("panics task failed") && err.contains("boom"),
            "{err}"
        );
        tasks.shutdown().await.expect("idle task aborts cleanly");
    }

    #[test]
    fn supported_major_spans_audited_sinval_layouts() {
        assert_eq!(supported_major(160_004).unwrap(), 16);
        assert_eq!(supported_major(190_000).unwrap(), 19);
        let err = supported_major(150_008).unwrap_err().to_string();
        assert_eq!(
            err,
            "source PG major 15 unsupported (commit-record sinval layout audited for 16-19)"
        );
    }

    #[tokio::test]
    async fn adopted_task_panic_propagates_and_fails_shutdown() {
        let mut tasks = SessionTasks::default();
        tasks.adopt("quits", tokio::spawn(async {}));
        let err = tasks.exited().await.unwrap().to_string();
        assert_eq!(err, "quits task exited");
        tasks.adopt("panics", tokio::spawn(async { panic!("boom") }));
        let err = tasks.exited().await.unwrap().to_string();
        assert!(
            err.starts_with("panics task failed") && err.ends_with("\"boom\""),
            "{err}"
        );
        tasks.spawn("idle", std::future::pending());
        tasks.spawn("panics", async { panic!("boom") });
        tokio::task::yield_now().await;
        let err = tasks.shutdown().await.unwrap_err().to_string();
        assert!(
            err.starts_with("panics task failed") && err.ends_with("\"boom\""),
            "{err}"
        );
    }

    #[tokio::test]
    async fn task_stopped_prefers_fatal_over_join_result() {
        let fatal = walshadow::pipeline::Fatal::new();
        let exited = task_stopped("gc", Ok(()), &fatal).to_string();
        assert_eq!(exited, "gc task exited");
        let panicked = tokio::spawn(async { panic!("boom") }).await;
        let failed = task_stopped("gc", panicked, &fatal).to_string();
        assert!(failed.starts_with("gc task failed") && failed.contains("boom"));
        fatal.set("disk gone".into());
        let panicked = tokio::spawn(async { panic!("boom") }).await;
        assert_eq!(
            task_stopped("gc", panicked, &fatal).to_string(),
            "gc failed: disk gone"
        );
    }

    #[tokio::test]
    async fn finished_task_fails_on_panic_or_fatal() {
        let fatal = walshadow::pipeline::Fatal::new();
        task_finished("fsync", Ok(()), &fatal).unwrap();
        let panicked = tokio::spawn(async { panic!("boom") }).await;
        let err = task_finished("fsync", panicked, &fatal).unwrap_err();
        assert!(err.to_string().starts_with("fsync task failed"), "{err}");
        fatal.set("syncfs out-dir: EIO".into());
        let err = task_finished("fsync", Ok(()), &fatal).unwrap_err();
        assert_eq!(err.to_string(), "fsync failed: syncfs out-dir: EIO");
    }

    #[tokio::test]
    async fn walreceiver_wait_names_the_walsender_when_none_attaches() {
        let state = Mutex::new(walshadow::shadow_stream::ShadowStreamState::new(
            1,
            "1".into(),
            0,
            1,
        ));
        let addr = "127.0.0.1:5555".parse().unwrap();
        let shutdown = CancellationToken::new();
        let err = await_walreceiver(&state, addr, Duration::ZERO, &shutdown)
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("walsender 127.0.0.1:5555 within 0s (accepted 0"),
            "{err}"
        );
    }

    #[test]
    fn followed_databases_fit_bridge_seats() {
        assert_eq!(followed_databases(None, "app").unwrap(), ["app"]);
        let seats = walshadow::bridge::MAX_BRIDGE_DATABASES;
        let mut cfg = EmitterConfig {
            databases: (0..seats).map(|i| format!("db{i}")).collect(),
            ..EmitterConfig::default()
        };
        assert_eq!(
            followed_databases(Some(&cfg), "app").unwrap(),
            cfg.databases
        );
        cfg.databases.push("one-more".into());
        assert!(followed_databases(Some(&cfg), "app").is_err());
    }

    #[test]
    fn boot_manifest_refuses_foreign_and_unreadable_unless_overridden() {
        let spill = std::path::Path::new("/spill");
        let identity = |system_id| manifest::SourceIdentity {
            system_id,
            timeline: 1,
            timeline_begin: Pos::ZERO,
        };
        let foreign = || manifest::ManifestError::ForeignSource {
            stored: identity(1),
            live: identity(2),
        };
        assert!(boot_manifest(Ok(None), false, spill).unwrap().is_none());
        let err = boot_manifest(Err(foreign()), true, spill).unwrap_err();
        assert!(err.to_string().contains("another source"), "{err}");
        let unreadable = || Err(manifest::ManifestError::Version(99));
        let err = boot_manifest(unreadable(), false, spill).unwrap_err();
        assert!(
            err.to_string().contains("--ignore-cursor / --start-lsn"),
            "{err}"
        );
        assert!(boot_manifest(unreadable(), true, spill).unwrap().is_none());
    }

    #[test]
    fn start_branch_needs_lineage_unless_cursor_ignored() {
        let promoted = || TimelineHistory::parse(2, b"1\t0/3000000\tpromoted\n").unwrap();
        let aligned = Pos::new(0x400_0000);
        let mut history = promoted();
        assert_eq!(start_branch(&mut history, 2, aligned, 2, false).unwrap(), 2);
        assert_eq!(history.entries().len(), 2);
        let err = start_branch(&mut history, 3, aligned, 2, false).unwrap_err();
        assert!(
            err.to_string().starts_with("timeline_not_descendant"),
            "{err}"
        );
        assert_eq!(start_branch(&mut history, 3, aligned, 2, true).unwrap(), 2);
        assert_eq!(
            history.entries().len(),
            1,
            "re-baselined onto a root history"
        );
    }

    #[test]
    fn branch_begin_refuses_sibling_unless_cursor_ignored() {
        assert!(check_branch_begin(2, 0, 0x300_0000, false).is_ok());
        assert!(check_branch_begin(1, 0, 0, false).is_ok());
        assert!(check_branch_begin(2, 0x300_0000, 0x300_0000, false).is_ok());
        let err = check_branch_begin(2, 0x300_0000, 0x500_0000, false).unwrap_err();
        assert!(err.to_string().starts_with("sibling_branch"), "{err}");
        assert!(check_branch_begin(2, 0x300_0000, 0x500_0000, true).is_ok());
    }
}
