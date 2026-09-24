//! One streaming session — source connect through pump loop, shutdown, and
//! everything the status tick publishes along the way.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use ahash::{HashMap, HashMapExt};
use anyhow::{Context, Result};
use tokio::sync::{Mutex, watch};
use tokio_postgres::types::Oid;
use tokio_util::sync::CancellationToken;
use walrus::pg::backup::format_pg_lsn;
use walshadow::archive::Archive;
use walshadow::boundary_hold::{BoundaryGateConfig, BoundaryHoldSink, CatalogBoundaryGate};
use walshadow::ch_emitter::{EmitterConfig, EmitterStats};
use walshadow::config::{ConfigResolver, SourceConn};
use walshadow::manifest;
use walshadow::metrics::{MetricsRegistry, RateEstimator};
use walshadow::pg::socket_conninfo;
use walshadow::pipeline::{PipelineConfig, TailKind};
use walshadow::pos::{
    EmitterAck, FilterDurable, Floor, Monotone, Pos, ShadowFlush, ShadowReplay, SourceReceived,
};
use walshadow::queueing_record_sink::{
    DEFAULT_QUEUEING_BATCH_SIZE, DEFAULT_QUEUEING_RECORD_SINK_CAPACITY, QueueingRecordSink,
};
use walshadow::record::{MetricsRecordSink, WAL_SEG_SIZE};
use walshadow::retention::max_segment_end;
use walshadow::segment_sink::{DirSegmentSink, SegFsync};
use walshadow::source_db::{DbLink, DbLinkConfig, SourceDb, SourceDbs};
use walshadow::source_feed::{SourceEvent, SourceFeed, StandbyStatus};
use walshadow::timeline::TimelineHistory;
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
    DbMetricSources, DrainResident, ShadowMetricsView, SourceSwap, StageCounters, TimelineView,
    populate_metrics,
};
use crate::runtime_cfg::{or_signal, sighup_reload};
use crate::shadow_proc::{
    OwnedShadow, ShadowLifecycle, bridge_pool_size, build_owned_shadow, open_shadow_sql_client,
    probe_blocking, start_owned_shadow, walsender_primary_conninfo,
};
use crate::sinks::{DaemonSinks, DecoderXactPair};
use crate::source_db::{
    DescLogInputs, SourceDbInputs, build_source_db, metrics_only_db, open_db_desc_log,
    open_source_sql_client,
};
use crate::source_recovery::{
    BARRIER_LOG_INTERVAL, FORK_FENCE_DRAIN, PROMOTION_POLL, PromotionGate, ReconnectBackoff,
    SOURCE_SWAP_RETRY, SourcePath, SourceRecovery, commit_fork_resume, connect_source_waiting,
    promotion_gate, resume_manifest, resume_source_feed, stream_branch, swap_reason,
};

pub(crate) async fn run_session(
    args: &Args,
    metrics: &MetricsRegistry,
    reloader: &Arc<walshadow::control::Reloader>,
    sighup: tokio::signal::unix::Signal,
    shutdown: &CancellationToken,
) -> Result<()> {
    // Clone the Arc-backed registry so the body's `&metrics` uses are unchanged.
    let metrics = metrics.clone();
    let mut tasks = SessionTasks::default();
    tasks.spawn("sighup reload", sighup_reload(sighup, reloader.clone()));

    let merged: toml::Table = match args.ch_config.as_deref() {
        Some(p) => walshadow::ch_emitter::load_effective(p, cli_base(args))
            .await
            .with_context(|| format!("load config {}", p.display()))?,
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
    let mut feed = or_signal(
        shutdown,
        connect_source_waiting(args, &mut source_conn, &mut cfg),
    )
    .await?;

    let ident = feed.identify_system().await.context("IDENTIFY_SYSTEM")?;
    tracing::info!(
        target: "walshadow",
        sysid = %ident.sysid,
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
        walshadow::ch_ddl::ensure_boot_database(cfg)
            .await
            .with_context(|| format!("reach ClickHouse {}:{}", cfg.host, cfg.port))?;
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
    // Followed databases, in bridge socket order: `[source] dbname` plus
    // every database-prefixed config key
    let source_databases: Vec<String> = match ch_config.as_ref() {
        Some(cfg) if !cfg.databases.is_empty() => cfg.databases.clone(),
        _ => vec![source_conn.dbname.clone()],
    };
    anyhow::ensure!(
        source_databases.len() <= walshadow::bridge::MAX_BRIDGE_DATABASES,
        "config names {} source databases, the shadow bridge seats {}",
        source_databases.len(),
        walshadow::bridge::MAX_BRIDGE_DATABASES,
    );
    let bootstrap_plan = resolve_bootstrap(args, ch_config.as_ref())?;
    let shadow_start = resolve_shadow_start(args, bootstrap_plan.mode)?;
    if ch_config.as_ref().is_some_and(|c| c.toast.mode.is_shadow())
        && let ShadowStart::Resume(dir) = &shadow_start
    {
        walshadow::filter::shadow_relations::ShadowRelations::load(dir).await?;
    }
    let bridge_workers = bridge_pool_size(ch_config.as_ref());
    // Slot before bootstrap
    if let Some(slot) = source_conn.slot.as_deref() {
        feed.ensure_physical_slot(slot)
            .await
            .with_context(|| format!("ensure physical replication slot {slot}"))?;
        tracing::info!(target: "walshadow", slot, "physical replication slot ready");
    }
    // Uptime anchors here, ahead of bootstrap: an initial load is part of the
    // session, and a `t0` that only starts at the status loop reads as a
    // counter reset to a scraper watching through it
    let start_instant = Instant::now();
    // One emitter-counter handle for both phases. Bootstrap's insert tail and
    // the streaming pipeline write the same series, so sharing it is what
    // keeps inserter and TOAST totals from resetting at handoff
    let emitter_stats = Arc::new(EmitterStats::default());
    let mut bootstrap_metrics: Option<BootstrapMetrics> = None;
    let mut bootstrap_handoff: Option<BootstrapHandoff> = if shadow_start.bootstraps() {
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
        let previous = if let ShadowStart::Rebootstrap(_, marker) = &shadow_start {
            Some(marker.clone())
        } else {
            None
        };
        let (handoff, stage) = or_signal(
            shutdown,
            run_bootstrap(
                &cfg,
                &mut feed,
                args,
                &bootstrap_plan,
                previous,
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
            walshadow::ops::stages::SHADOW_REPLAY
                .measure(start_owned_shadow(
                    &owned.shadow,
                    bootstrap_end_lsn,
                    Duration::from_secs(args.bootstrap_shadow_replay_timeout),
                    args.keep_shadow_running,
                ))
                .await?;
            owned
        }
    };
    let shadow_lifecycle =
        ShadowLifecycle::spawn(owned, walsender_primary_conninfo(args.walsender_bind));
    let archive = ch_config
        .as_ref()
        .and_then(|c| c.backup.clone())
        .map(Archive::open)
        .transpose()?;
    let start_lsn_override: Option<Pos<Floor>> = args
        .start_lsn
        .as_deref()
        .map(|s| walshadow::pg::parse_pg_lsn(s).context("--start-lsn"))
        .transpose()?
        .map(Pos::new);

    let live_identity = manifest::SourceIdentity {
        system_id: ident.sysid.parse().context("IDENTIFY_SYSTEM sysid")?,
        timeline: ident.timeline,
        timeline_begin: Pos::ZERO,
    };
    // Identity gate runs before `--ignore-cursor`: the flag discards resume
    // LSNs, not artifact ownership. Foreign system_id is fatal regardless
    // (retire/backfill ledgers would act on another cluster's state). A newer
    // live timeline is a promotion, proved against the source's history below.
    let manifest_at_boot: Option<manifest::Manifest> =
        match manifest::load(&args.spill_dir, &live_identity).await {
            Ok(m) => m,
            Err(e @ manifest::ManifestError::ForeignSource { .. }) => {
                anyhow::bail!("{e}");
            }
            Err(e) if args.ignore_cursor || start_lsn_override.is_some() => {
                tracing::warn!(
                    target: "walshadow::manifest",
                    error = %e,
                    spill_dir = %args.spill_dir.display(),
                    "manifest unreadable; operator override discards it",
                );
                None
            }
            Err(e) => {
                anyhow::bail!(
                    "manifest at {} unreadable: {e}; restore it, or authorize \
                     recovery with --ignore-cursor / --start-lsn",
                    manifest::manifest_path(&args.spill_dir).display(),
                );
            }
        };
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
    let start_timeline = match history.resume_branch(stored_timeline, aligned.get(), WAL_SEG_SIZE) {
        Some(tli) => tli,
        None if args.ignore_cursor => {
            let found = history.tli_of_segment(aligned.get(), WAL_SEG_SIZE);
            tracing::warn!(
                target: "walshadow",
                stored_timeline,
                live_timeline = ident.timeline,
                serves_start = found,
                "--ignore-cursor adopts the live timeline without a lineage proof",
            );
            history = TimelineHistory::root(ident.timeline);
            ident.timeline
        }
        None => anyhow::bail!(
            "timeline_not_descendant: stored timeline {stored_timeline} does not reach \
             {} on live timeline {}'s history (it serves {:?}); \
             --ignore-cursor re-baselines onto the live branch",
            aligned,
            ident.timeline,
            history.tli_of_segment(aligned.get(), WAL_SEG_SIZE),
        ),
    };
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
    match stored_begin {
        0 if stored_timeline > 1 => tracing::warn!(
            target: "walshadow",
            stored_timeline,
            live_begin = %format_pg_lsn(live_begin),
            "manifest records no switchpoint for its branch, so a sibling sharing \
             that number cannot be refused until the next manifest write",
        ),
        0 => {}
        begin if begin != live_begin && !args.ignore_cursor => anyhow::bail!(
            "sibling_branch: source places timeline {stored_timeline} at {}, \
             walshadow's artifacts came off it from {}; the branch behind them is \
             absent from this source's history",
            format_pg_lsn(live_begin),
            format_pg_lsn(begin),
        ),
        begin if begin != live_begin => tracing::warn!(
            target: "walshadow",
            stored_timeline,
            stored_begin = %format_pg_lsn(begin),
            live_begin = %format_pg_lsn(live_begin),
            "--ignore-cursor adopts a branch that begins somewhere else",
        ),
        _ => {}
    }
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

    let mut stream = WalStream::new(start_timeline, WAL_SEG_SIZE, aligned)?;
    let prefix_dirs = [args.out_dir.clone(), shadow_start.data_dir().join("pg_wal")];
    stream.preserve_resume_prefix(&prefix_dirs).await?;
    // Shadow must attach to this listener before catalog replay can advance
    let mut shadow_boot = walshadow::shadow_stream::ShadowStreamState::new(
        history.shadow_boot_branch(stored_timeline, aligned.get(), start_timeline),
        ident.sysid.clone(),
        aligned.get(),
        args.walsender_slow_threshold,
    );
    seed_shadow_branches(
        &mut shadow_boot,
        &mut feed,
        &history,
        &args.out_dir,
        start_timeline,
    )
    .await?;
    let shadow_state = Arc::new(Mutex::new(shadow_boot));
    let walsender_addr = args.walsender_bind;
    let walsender_task = walshadow::shadow_stream::spawn_listener(
        walshadow::shadow_stream::WalSenderAddr::Tcp(walsender_addr),
        shadow_state.clone(),
        Duration::from_millis(50),
    )
    .await
    .with_context(|| format!("bind walsender at {walsender_addr}"))?;
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
    let mut db_conns: Vec<DbLink> = Vec::with_capacity(source_databases.len());
    for (index, name) in source_databases.iter().enumerate() {
        // Same document per database, scoped to that database's entries
        let emitter = match ch_config.as_ref() {
            Some(_) => Some(finish_ch_config(
                EmitterConfig::for_database(&merged, name)
                    .with_context(|| format!("parse ch config for database {name}"))?,
                args,
            )),
            None => None,
        };
        let conninfo = socket_conninfo(socket_dir, args.shadow_port, &args.shadow_user, name);
        db_conns.push(
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
    let primary_index = source_databases
        .iter()
        .position(|db| *db == source_conn.dbname)
        .unwrap_or(0);
    let shadow_conninfo = socket_conninfo(
        args.shadow_socket_dir
            .to_str()
            .context("shadow-socket-dir not UTF-8")?,
        args.shadow_port,
        &args.shadow_user,
        &source_conn.dbname,
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
            &source_conn.dbname,
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
        for conn in &db_conns {
            if conn.name == source_conn.dbname {
                continue;
            }
            let Some(cfg) = &conn.emitter else {
                continue;
            };
            let client = open_source_sql_client(&source_conn, &conn.name)
                .await
                .with_context(|| format!("source sql for database {}", conn.name))?;
            report.errors.extend(
                walshadow::preflight::mapped_relations(&client, cfg)
                    .await
                    .with_context(|| format!("pre-flight probe for database {}", conn.name))?,
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
    // Spill dir wiped every startup: cursor file commits drains
    // atomically, so leftover spill from a prior crash is redundant or stale.
    let xact_buf_cfg = XactBufferConfig {
        xact_buffer_max: args.xact_buffer_max,
        ..XactBufferConfig::new(args.spill_dir.clone())
    };
    let xact_buffer = XactBuffer::new(xact_buf_cfg).context("init xact buffer / spill dir")?;
    xact_buffer
        .clear_spill_dir()
        .await
        .context("clear stale spill files")?;
    let xact_buffer = Arc::new(Mutex::new(xact_buffer));
    tracing::info!(
        target: "walshadow",
        spill_dir = %args.spill_dir.display(),
        xact_buffer_max = args.xact_buffer_max,
        "spill dir ready",
    );

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
    let source_major = (feed.server_version_num() / 10000) as u32;
    anyhow::ensure!(
        (16..=19).contains(&source_major),
        "source PG major {source_major} unsupported (commit-record sinval layout audited for 16-19)",
    );
    stream
        .filter_mut()
        .set_target_dbs(db_conns.iter().map(|conn| conn.oid));
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
    let smgr_markers = stream.filter_mut().smgr_markers();
    // One log per database, each in its own spill subdirectory; the primary
    // keeps the spill root so a single-database resume reads where it wrote
    let mut desc_logs: Vec<Arc<walshadow::desc_log::DescriptorLog>> =
        Vec::with_capacity(db_conns.len());
    for (i, conn) in db_conns.iter().enumerate() {
        let dir = if i == primary_index {
            args.spill_dir.clone()
        } else {
            let dir = args.spill_dir.join(format!("db-{}", conn.oid));
            tokio::fs::create_dir_all(&dir)
                .await
                .with_context(|| format!("create descriptor log dir {}", dir.display()))?;
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
                    system_id: ident.sysid.clone(),
                    // Resume branch, which a crossing moves without moving the
                    // log: the stored header names wherever the log last
                    // rewrote itself, so `lineage` is what places it
                    timeline: start_timeline,
                    db_oid: conn.oid,
                    wal_seg_size: WAL_SEG_SIZE as u32,
                },
                lineage: &lineage,
                manifest_present: manifest_at_boot.is_some(),
                start_lsn_override,
                raw_start,
                aligned,
            })
            .await?,
        );
    }
    let desc_log = desc_logs[primary_index].clone();
    let all_desc_logs = walshadow::desc_log::DescriptorLogs::new(desc_logs.clone());

    // Txn-span registry, shared by pump + decoder; `Some` only with OTLP on.
    let span_registry =
        if args.otlp_endpoint.is_some() || std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok() {
            Some(xact_buffer.lock().await.span_registry())
        } else {
            None
        };
    let mut decoder = BufferingDecoderSink::new(all_desc_logs.clone(), xact_buffer.clone());
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
        // Cluster-wide knobs come off the primary's copy: every database
        // parsed the same `[ch]`, `[memory]` and `[stream]` document
        let mut emitter_cfg = db_conns[primary_index]
            .emitter
            .clone()
            .expect("[ch] present");
        let addr = format!("{}:{}", emitter_cfg.host, emitter_cfg.port);
        let stats = emitter_stats.clone();
        emitter_stats_handle = Some(stats.clone());
        // One validated resident-payload pool for the pipeline and every
        // concurrent backup pass
        let pipeline_budget =
            walshadow::pipeline::build_budget(&emitter_cfg, emitter_cfg.decoder_pool_size)
                .map_err(|e| anyhow::anyhow!("memory budget: {e}"))?;
        let mut dbs: Vec<Arc<SourceDb>> = Vec::with_capacity(db_conns.len());
        let mut applicators: HashMap<Oid, walshadow::ch_ddl::DdlApplicator> = HashMap::new();
        let mut backfillers: HashMap<Oid, Arc<dyn walshadow::opt_in::Backfiller>> = HashMap::new();
        let mut resolvers: Vec<Arc<ConfigResolver>> = Vec::with_capacity(db_conns.len());
        // One claim per destination across every database, so `replicate_all`
        // cannot name one ClickHouse table from two source tables
        let targets = Arc::new(walshadow::mapping::TargetOwners::default());
        for (i, conn) in db_conns.iter().enumerate() {
            let built = build_source_db(SourceDbInputs {
                args,
                conn,
                primary: i == primary_index,
                targets: &targets,
                desc_log: &desc_logs[i],
                spill_dir: if i == primary_index {
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
            .with_context(|| format!("wire source database {}", conn.name))?;
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
            if let Some(applicator) = built.applicator {
                applicators.insert(conn.oid, applicator);
            }
            if let Some(backfiller) = built.backfiller.clone() {
                backfillers.insert(conn.oid, backfiller as _);
                copy_backfillers.insert(conn.oid, built.backfiller.expect("just cloned"));
            }
            if let Some(resolver) = &built.db.resolver {
                resolvers.push(resolver.clone());
                resolvers_by_db.insert(conn.oid, resolver.clone());
            }
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
            dbs: Arc::new(SourceDbs::new(dbs, primary_db_oid)),
            oracle: oracle.clone(),
            applicators,
            tail: TailKind::ClickHouse,
            buffer: xact_buffer.clone(),
            subxact_tracker: Arc::new(Mutex::new(SubxactTracker::new())),
            pending: pending_catalog.clone(),
            stats: stats.clone(),
            span_registry: span_registry.clone(),
            backfillers,
            retires,
            pending_rows,
            resume_floor: resume_floor.clone(),
            budget: Some(pipeline_budget),
        }
    } else {
        // Metrics-only (no CH): the identical pipeline with a null tail —
        // zero CH connections, no DDL applicator, no oracle (nothing ships,
        // PgPending stays raw). The empty mapping routes nothing, so seqs
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
            dbs: Arc::new(SourceDbs::new(dbs, primary_db_oid)),
            oracle: None,
            applicators: HashMap::new(),
            tail: TailKind::Null,
            buffer: xact_buffer.clone(),
            subxact_tracker: Arc::new(Mutex::new(SubxactTracker::new())),
            pending: pending_catalog.clone(),
            stats: Arc::new(EmitterStats::default()),
            span_registry: span_registry.clone(),
            backfillers: HashMap::new(),
            retires,
            pending_rows: walshadow::visibility_pending::PendingLedger::empty().shared(),
            resume_floor: resume_floor.clone(),
            budget: None,
        }
    };

    let (mut reorder_sink, pipeline_handle) = pcfg
        .spawn(emitter_ack.clone())
        .await
        .context("spawn decode+insert pipeline")?;
    let ack_probe = pipeline_handle.ack_probe.clone();
    reorder_sink
        .flush_due_retires()
        .await
        .context("boot flush of due toast-mirror retires")?;
    reorder_sink
        .settle_pending_boot(Some(&args.bootstrap_shadow_data_dir))
        .await
        .context("boot settle of pending backup rows")?;
    reorder_sink
        .apply_boot_events(desc_log.active_present_at(raw_start.get()), raw_start.get())
        .await
        .context("boot Added pass over descriptor log")?;
    let decoder_xact = QueueingRecordSink::spawn(
        DecoderXactPair {
            decoder,
            xact_drain: reorder_sink,
        },
        decoder_batch_size,
        decoder_queue_capacity,
        span_registry.clone(),
    );
    let boundary_gate = CatalogBoundaryGate::new(
        shadow_state.clone(),
        BoundaryGateConfig {
            hold_timeout: Duration::from_secs(args.catalog_hold_timeout),
            ..BoundaryGateConfig::default()
        },
    );
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
    let decoder_xact = BoundaryHoldSink::new(decoder_xact, boundary_gate).with_capture(captures);
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
    let mut fsync_task = spawn_segment_fsync(
        args.out_dir.clone(),
        fsync_rx,
        durable_lsn.clone(),
        fsync_fatal.clone(),
    );
    let mut segment_sink =
        DirSegmentSink::with_durability(args.out_dir.clone(), WAL_SEG_SIZE, fsync_tx)
            .context("open out-dir")?;
    // Descriptor-log GC off the pump task: the pump publishes each persisted
    // floor, the task compacts. Coalesces by construction — a watch holds
    // only the latest floor.
    let gc_fatal = walshadow::pipeline::Fatal::new();
    // Pruner's own cell, not `resume_floor`: dropping it is what tells the gc
    // task the session is done
    let gc_floor = Monotone::<Floor>::default();
    let mut gc_task = spawn_desc_log_gc(desc_log.clone(), gc_floor.watch(), gc_fatal.clone());
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
    {
        let timeout = Duration::from_secs(args.walsender_connect_timeout);
        let start = Instant::now();
        loop {
            let agg = shadow_state.lock().await.aggregate();
            if agg.active_connections > 0 {
                break;
            }
            // `accepted` separates "shadow never dialed" from "shadow dialed
            // and stalled in the handshake" — the latter reads as the former
            // without it, since only START_REPLICATION registers a connection
            anyhow::ensure!(
                start.elapsed() < timeout,
                "no walreceiver streaming from walsender {walsender_addr} within \
                 {}s (accepted {}, none sent START_REPLICATION); catalog-boundary \
                 holds require a live wire — point shadow's primary_conninfo here \
                 or raise --walsender-connect-timeout",
                args.walsender_connect_timeout,
                agg.accepted_total,
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
    }

    let mut source_recovery = SourceRecovery {
        system_id: live_identity.system_id,
        status_interval: Duration::from_secs(args.status_interval),
        backup: archive.as_ref(),
        floor: &resume_floor,
        prefetch: usize::from(args.archive_prefetch),
        backoff: ReconnectBackoff::default(),
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
        path = source_recovery
            .attempt(Some(e), &source_conn, &history, &stream, &mut feed)
            .await?;
    }

    let mut segments_shipped = 0u64;
    let mut prev_dispatched = stream.dispatched_lsn();
    let mut rate_estimator = RateEstimator::default();
    // Manifest write cadence. Slot safety doesn't ride on it: advertised
    // flush_lsn is capped at the persisted floor below, so a lagging write
    // only delays slot advance, never overshoots it.
    let cursor_write_interval = Duration::from_secs(args.status_interval);
    let mut last_cursor_write: Option<Instant> = None;
    // Fast metrics-refresh tick (decoupled from cursor/status): an idle source
    // would otherwise freeze the /metrics snapshot while the pipeline drains.
    let metrics_tick = Duration::from_millis(250);
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
    let pump_config_rx = config_resolvers.first().map(|r| r.subscribe());
    let mut swap = SourceSwap::default();
    // Frozen when the pump observes a pause, so a promotion decision reads a
    // frontier that cannot move under it. Cleared on resume: a value left over
    // from an earlier pause is as misleading as a live one
    let mut pause_frontier: Option<(u64, u64)> = None;
    // A restart mid-pause re-freezes both numbers, conservatively but not
    // identically, so the pair an operator already read has to be read again
    let mut pause_refrozen = false;
    let mut ever_unpaused = false;
    // Step 5's answer, refreshed while paused off the endpoint the pump holds
    let mut promotion = PromotionGate::default();
    let mut promotion_polled_at: Option<Instant> = None;
    let switchover = Switchover {
        system_id: live_identity.system_id,
        out_dir: &args.out_dir,
        shadow_state: &shadow_state,
        backup: archive.as_ref(),
    };
    let mut timeline_stats = TimelineStats {
        // Off the chain, so a restart after a crossing keeps reporting the fork
        // it resumed across instead of zero
        switch_lsn: history.begin_of(start_timeline).unwrap_or(0),
        ..TimelineStats::default()
    };
    // The ancestor ended and the descendant has not been adopted yet. Survives
    // iterations so a source error mid-crossing retries the crossing: at the
    // ancestor's switchpoint an ordinary reconnect has nothing to ask for
    let mut crossing = CrossingState::default();
    let mut barrier_logged: Option<Instant> = None;
    let shutdown_reason = 'pump: loop {
        // Nothing resumes at a switchpoint, not archive nor redial, only a crossing
        if !matches!(path, SourcePath::Live)
            && history.branch_exhausted(stream.timeline(), stream.next_lsn().get())
        {
            path = SourcePath::Live;
            crossing.ancestor_ended(true);
        }
        let paused = pump_config_rx
            .as_ref()
            .map(|rx| rx.borrow().paused)
            .unwrap_or(false);
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
                .attempt(None, &source_conn, &history, &stream, &mut feed)
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
                Ok(swapped) => {
                    feed = swapped;
                    path = SourcePath::Live;
                    swap.settled();
                    swap.swaps += 1;
                    tracing::info!(
                        target: "walshadow",
                        endpoint = source_conn.endpoint(),
                        resume_lsn = %stream.next_lsn(),
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
        match (paused, pause_frontier) {
            (true, None) => {
                pause_frontier = Some((
                    stream.next_lsn().get(),
                    received.get().max(stream.next_lsn().get()),
                ));
                // A pause this process never saw lifted was taken before it
                // booted, so these two numbers replace ones an operator may
                // already hold. Both re-freeze conservatively — consumed drops
                // back to the floor, received re-derives from the live head —
                // but a promotion decision has to be taken from the pair on
                // offer now (architecture/recovery.md)
                pause_refrozen = !ever_unpaused;
                let (consumed, head) = pause_frontier.expect("just frozen");
                tracing::info!(
                    target: "walshadow",
                    pause_consumed_lsn = %format_pg_lsn(consumed),
                    pause_received_lsn = %format_pg_lsn(head),
                    refrozen = pause_refrozen,
                    "pause observed — frontier frozen",
                );
            }
            (false, Some(_)) => {
                pause_frontier = None;
                pause_refrozen = false;
            }
            _ => {}
        }
        ever_unpaused |= !paused;
        // Step 5 of the protocol, answered off the connection step 4's repoint
        // already moved onto the target: replay, receive, and recovery state
        // beside the frozen frontier they have to reach
        // (architecture/recovery.md)
        if !paused {
            promotion = PromotionGate::blocked("not_paused");
            promotion_polled_at = None;
        } else if promotion_polled_at.is_none_or(|t| t.elapsed() >= PROMOTION_POLL) {
            promotion_polled_at = Some(Instant::now());
            promotion = match tokio::time::timeout(
                PROMOTION_POLL,
                promotion_gate(&mut feed, pause_frontier),
            )
            .await
            {
                Ok(gate) => gate,
                Err(_) => {
                    feed.drop_sql_client();
                    PromotionGate::unreachable()
                }
            };
        }
        let (shadow_agg, shadow_served_tli) = {
            let state = shadow_state.lock().await;
            (state.aggregate(), state.timeline)
        };
        if let Some(apply) = shadow_agg.min_apply_lsn {
            shadow_replay_lsn.join(apply);
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
            manifest::write(&args.spill_dir, &cur)
                .await
                .context("write resume manifest")?;
            last_cursor_write = Some(Instant::now());
            // Publish only after persist: pruners cut against what a
            // crash-now restart actually resumes from.
            resume_floor.join(cur.floor);
            // Descriptor log prunes against the same floor, off this task: a
            // compaction rewrites the whole ckpt inline and would stall WAL
            // consumption past the source's wal_sender_timeout
            gc_floor.join(cur.floor);
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
        let chunk = tokio::select! {
            biased;
            () = shutdown.cancelled() => break "signal",
            err = tasks.exited() => return Err(err),
            res = &mut fsync_task => return Err(task_stopped("segment fsync", res, &fsync_fatal)),
            res = &mut gc_task => return Err(task_stopped("descriptor log gc", res, &gc_fatal)),
            // Surfaced by the check after the crossing step
            () = pipeline_handle.fatal.wait() => None,
            // Idle tick so metrics/cursor keep tracking, and so a `paused` flip
            // is picked up promptly.
            _ = tokio::time::sleep(metrics_tick) => None,
            // Paused: stop consuming source WAL (idle); resume re-enables this
            // arm and the pump continues from the same LSN. A pending crossing
            // also parks it — that connection is out of COPY until the
            // descendant is requested.
            result = async { path.archive().expect("guarded by arm").next().await },
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
                            tracing::warn!(
                                target: "walshadow",
                                error = %e,
                                resume_lsn = %stream.next_lsn(),
                                "source stream error — recovering",
                            );
                            e
                        }
                        _ => {
                            tracing::info!(
                                target: "walshadow",
                                resume_lsn = %stream.next_lsn(),
                                "source shut down its walsender — reconnecting",
                            );
                            anyhow::anyhow!("source walsender exited")
                        }
                    };
                    path = source_recovery
                        .attempt(Some(err), &source_conn, &history, &stream, &mut feed)
                        .await?;
                    // Recovery dials the live endpoint, so a queued swap is done
                    swap.settled();
                    None
                }
            },
        };
        let server_end = chunk
            .as_ref()
            .map(|c| c.server_wal_end)
            .unwrap_or(received.get());
        if let Some(chunk) = chunk {
            let replay_started = Instant::now();
            stream
                .push(
                    chunk.start_lsn,
                    chunk.data,
                    &mut record_sink,
                    &mut segment_sink,
                )
                .await?;
            if archived_segment {
                metrics
                    .update(|snap| {
                        snap.archive_wal_segments_total += 1;
                        snap.archive_replay_seconds_total += replay_started.elapsed().as_secs_f64();
                    })
                    .await;
            }
        }
        metrics
            .update(|snap| {
                snap.pump_queue_wait_seconds_total =
                    record_sink.decoder_xact.inner.send_wait_seconds();
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
            let ended = feed.end_historic_stream().await;
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
            // Both fork proofs read the decoder's view, so the pump-side queue
            // drains first: a record still in flight answers for a frontier the
            // decoder has not reached, which would read as a transaction left
            // open at the fork
            record_sink
                .decoder_xact
                .flush()
                .await
                .context("flush queueing decoder sink at the fork")?;
            let fence = Instant::now();
            let in_flight = loop {
                let n = record_sink.decoder_xact.in_flight();
                if n == 0 || fence.elapsed() >= FORK_FENCE_DRAIN {
                    break n;
                }
                tokio::select! {
                    () = shutdown.cancelled() => break 'pump "signal",
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
            };
            // Timeout stops queue drain only, fork guards remain authoritative
            if in_flight != 0 {
                tracing::warn!(
                    target: "walshadow",
                    in_flight,
                    waited = ?fence.elapsed(),
                    "fork fence gave up draining the pump queue — guards decide",
                );
            }
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
            let waiting_on = walshadow::transition::ForkBarrier {
                resume_safe_lsn: resume_safe,
                shadow_apply_lsn: shadow_agg.min_apply_lsn,
                filter_durable: durable,
                floor: resume_floor.get(),
            }
            .pending(Pos::new(probed.switch_lsn), WAL_SEG_SIZE);
            if let Some(wait) = waiting_on {
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
            } else {
                barrier_logged = None;
                let commit = async |resume: walshadow::transition::ForkResume| {
                    commit_fork_resume(
                        &args.spill_dir,
                        &live_identity,
                        resume,
                        manifest::LsnSet {
                            // Fork cannot precede last observed source head
                            source_received: received.max(resume.switch_lsn.retag()),
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
        let now_dispatched = stream.dispatched_lsn();
        let advanced = now_dispatched != prev_dispatched;
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
        let shadow_apply_lsn = shadow_agg.min_apply_lsn.map_or(0, Pos::get);
        let lag_bytes = received.get().saturating_sub(shadow_apply_lsn);
        rate_estimator.observe(Instant::now(), received.get());
        let lag_seconds = rate_estimator.seconds_for(lag_bytes);
        // Post-worker snapshots so the metric reflects what the worker
        // drained, not the top-of-iteration values.
        let emitter_ack_for_metric = emitter_ack.get();
        let drain_for_metric = xact_stats.drain_lsn;
        populate_metrics(
            &metrics,
            received,
            Pos::new(now_dispatched),
            shadow_replay,
            drain_for_metric,
            emitter_ack_for_metric,
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
                pause_frontier,
                pause_refrozen,
                wedge: crossing.wedge().cloned(),
                promotion,
            },
            ShadowMetricsView {
                apply_lag_bytes: lag_bytes,
                apply_lag_seconds: lag_seconds,
                active_connections: shadow_agg.active_connections as u64,
                dropped_total: shadow_agg.dropped_total,
            },
            &boundary_hold_stats,
            metrics_dbs.iter().map(DbMetricSources::series).collect(),
            StageCounters {
                emitter: emitter_stats,
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
    // Drop the sink (closes the fsync queue) and drain the fsync task so
    // sealed segments are durable
    drop(segment_sink);
    let res = fsync_task.await;
    if res.is_err() || fsync_fatal.is_set() {
        return Err(task_stopped("segment fsync", res, &fsync_fatal));
    }
    // Close the floor channel and join: nothing else may own desc_log.ckpt
    // after the session returns
    drop(gc_floor);
    let res = gc_task.await;
    if res.is_err() || gc_fatal.is_set() {
        return Err(task_stopped("descriptor log gc", res, &gc_fatal));
    }
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

/// Session tasks meant to run until shutdown; any exit before it is fatal
#[derive(Default)]
pub(crate) struct SessionTasks {
    set: tokio::task::JoinSet<()>,
    names: ahash::HashMap<tokio::task::Id, &'static str>,
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

    /// First task to stop, as the error naming it. Pending while none has
    async fn exited(&mut self) -> anyhow::Error {
        match self.set.join_next_with_id().await {
            Some(Ok((id, ()))) => anyhow::anyhow!("{} task exited", self.name(id)),
            Some(Err(e)) => anyhow::anyhow!("{} task failed: {e}", self.name(e.id())),
            None => std::future::pending().await,
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn session_tasks_name_the_task_that_stopped() {
        let mut tasks = SessionTasks::default();
        tasks.spawn("idle", std::future::pending());
        tasks.spawn("quits", async {});
        let err = tasks.exited().await.to_string();
        assert!(err.contains("quits task exited"), "{err}");
        tasks.spawn("panics", async { panic!("boom") });
        let err = tasks.exited().await.to_string();
        assert!(
            err.contains("panics task failed") && err.contains("boom"),
            "{err}"
        );
        tasks.shutdown().await.expect("idle task aborts cleanly");
    }
}
