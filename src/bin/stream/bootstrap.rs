//! Initial load: plan resolution, basebackup or object-store extraction into
//! a fresh shadow data dir, and greenfield backfill handoff to the pump.

use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use ahash::HashSet;
use anyhow::{Context, Result};
use std::fs;
use walrus::pg::backup::format_pg_lsn;
use walrus::pg::replication::base_backup::BaseBackupOpts;
use walrus::pg::replication::conn::PgConfig;
use walrus::pg::replication::tls::SslMode;
use walrus::time::Timestamp;
use walshadow::backfill::visibility_gate::{
    DeferredLane, GateStats, GreenfieldSink, PendingGate, resolve_greenfield, stream_phase,
};
use walshadow::backfill_bootstrap::{
    BootstrapConfig, BootstrapOutcome, BootstrapProgress, drain_backfill, seed_in_snapshot,
    spawn_greenfield_bootstrap,
};
use walshadow::backup_source::BackupSource;
use walshadow::backup_source_direct::DirectSource;
use walshadow::backup_source_object_store::ObjectStoreSource;
use walshadow::bootstrap_marker::{self, BootstrapMarker};
use walshadow::ch_emitter::{BootstrapMode, EmitterConfig, EmitterStats};
use walshadow::config::{CliOverrides, ConfigResolver, cli_over_toml};
use walshadow::decoder_sink::MetricsTupleObserver;
use walshadow::mapping::MappingHandle;
use walshadow::metrics::{MetricsRegistry, MetricsSnapshot};
use walshadow::pipeline::Fatal;
use walshadow::pipeline::tail::OwnedTail;
use walshadow::runtime_config::InitialLoadMode;
use walshadow::schema::{RelName, SchemaEvent};
use walshadow::source_feed::SourceFeed;
use walshadow::toast::ToastResolver;
use walshadow::visibility::PgXactPatch;

use crate::archive::fetch_wal_into_pg_wal;
use crate::args::{Args, cli_base};
use crate::metrics_publish::{DbMetricSources, StageCounters, stage_gauges};
use crate::shadow_proc::{OwnedShadow, bridge_pool_size, build_owned_shadow, start_owned_shadow};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BootstrapPlan {
    pub(crate) mode: BootstrapMode,
    pub(crate) backup_name: String,
    pub(crate) parallelism: Option<usize>,
    pub(crate) lanes: Option<usize>,
}

impl BootstrapPlan {
    /// Stream window live for Direct mode, replay hydrated WAL otherwise
    pub(crate) fn live_window_leg(&self, args: &Args) -> bool {
        self.mode == BootstrapMode::Direct && !args.bootstrap_wal_from_archive
    }
}

pub(crate) fn resolve_bootstrap(args: &Args, ch: Option<&EmitterConfig>) -> Result<BootstrapPlan> {
    let toml = ch.map(|c| &c.bootstrap);
    let mode = cli_over_toml(args.bootstrap_mode, toml.and_then(|b| b.mode))
        .unwrap_or(BootstrapMode::Direct);
    let backup_name = cli_over_toml(
        args.bootstrap_backup_name.clone(),
        toml.and_then(|b| b.backup_name.clone()),
    );
    let parallelism = cli_over_toml(
        args.bootstrap_object_store_parallelism,
        toml.and_then(|b| b.object_store_parallelism),
    )
    .map(NonZeroUsize::get);
    let lanes =
        cli_over_toml(args.bootstrap_lanes, toml.and_then(|b| b.lanes)).map(NonZeroUsize::get);

    if mode != BootstrapMode::ObjectStore {
        for (knob, set) in [
            ("backup_name", backup_name.is_some()),
            ("object_store_parallelism", parallelism.is_some()),
        ] {
            if set {
                tracing::warn!(
                    target: "walshadow::bootstrap",
                    knob,
                    ?mode,
                    "bootstrap {knob} ignored, it applies only to --bootstrap-mode object_store",
                );
            }
        }
    }

    Ok(BootstrapPlan {
        mode,
        backup_name: backup_name.unwrap_or_else(|| "LATEST".into()),
        parallelism,
        lanes,
    })
}

/// A data dir holding `PG_VERSION` was initialized by a prior bootstrap (or
/// external `initdb`), so the shadow can resume rather than reseed.
pub(crate) fn shadow_data_dir_initialized(dir: &std::path::Path) -> bool {
    dir.join("PG_VERSION").exists()
}

/// Run BASE_BACKUP into new shadow data dir and return backup `end_lsn`
/// Caller starts WAL pump from returned LSN, then starts and supervises
/// shadow in [`crate::run`]
/// Config, credential, and CH-endpoint failures are resolved before the data
/// dir is created, so they leave nothing behind. Once extraction starts, the
/// marker survives a failure; `previous` carries it back on the retry
/// [`BootstrapMode::ObjectStore`] gets, and is `None` for a first attempt
///
/// `ch_config` `Some`: bootstrap rows route through the shared insert tail
/// (synthetic INSERT `_lsn = start_lsn`, `_commit_ts = 0`, `_is_deleted = 0`).
/// `wait_through(K)` proves every bootstrap seq durable on CH before
/// teardown, so the WAL pump resumes against a fully-shipped baseline.
/// `None`: rows drain to a metrics-only observer via `drain_backfill`.
pub(crate) async fn run_bootstrap(
    src_cfg: &PgConfig,
    feed: &mut SourceFeed,
    args: &Args,
    plan: &BootstrapPlan,
    previous: Option<BootstrapMarker>,
    ch_config: Option<EmitterConfig>,
    observers: BootstrapObservers<'_>,
) -> Result<(BootstrapHandoff, BootstrapMetrics)> {
    let BootstrapObservers {
        metrics,
        emitter_stats,
        uptime_from,
    } = observers;
    let timing = walshadow::ops::stages::BOOTSTRAP.start();
    let bridge_workers = bridge_pool_size(ch_config.as_ref());
    let shadow_data_dir = args.bootstrap_shadow_data_dir.clone();

    // Never land a base backup onto a dir that already holds a cluster: a
    // `PG_VERSION` with no completion marker is a crashed bootstrap or a
    // foreign/externally-seeded dir. Overwriting it would be destructive and
    // non-recoverable — make the operator clear it (or use `--bootstrap-mode=off`
    // to resume an externally-managed shadow).
    if previous.is_none() && shadow_data_dir_initialized(&shadow_data_dir) {
        anyhow::bail!(
            "bootstrap: {} already holds a cluster (PG_VERSION present) but no completed-bootstrap \
             marker — provide an empty data dir to bootstrap, or --bootstrap-mode=off to resume it",
            shadow_data_dir.display(),
        );
    }

    // Seed catalog map inside a REPEATABLE READ snapshot. DDL between the
    // seed COMMIT and BASE_BACKUP's checkpoint window is operator-quiesced
    // per the bootstrap out-of-scope contract.
    let sql_client = feed
        .sql_client()
        .await
        .context("bootstrap: source sidecar sql client")?;
    let catalog_map = seed_in_snapshot(sql_client)
        .await
        .context("bootstrap: seed_in_snapshot")?;
    // The landing and `filter_landed_wal` must agree on what counts as
    // catalog, or redo re-creates a file the landing skipped
    let mut landing_tracker = walshadow::catalog_tracker::CatalogTracker::new();
    landing_tracker
        .seed_from_source(sql_client)
        .await
        .context("bootstrap: seed catalog filenodes")?;
    let catalog_filenodes: Vec<_> = landing_tracker.nodes().collect();
    // Filtered at one of two points depending on toast mode, never both:
    // shadow mode rewrites before its recovery starts mid-bootstrap, other
    // modes after the window leg has read the raw segments
    let mut landing_tracker = Some(landing_tracker);
    tracing::info!(
        target: "walshadow::bootstrap",
        relations = catalog_map.len(),
        catalog_filenodes = catalog_filenodes.len(),
        mode = ?plan.mode,
        shadow_data_dir = %shadow_data_dir.display(),
        "catalog map seeded",
    );

    type WalHydrate = (walrus::config::Settings, walrus::storage::DynStorage);
    let mut pinned_backup: Option<String> = None;
    let (source, mut wal_hydrate): (Box<dyn BackupSource>, Option<WalHydrate>) = match plan.mode {
        BootstrapMode::Direct => {
            let hydrate = if args.bootstrap_wal_from_archive {
                let settings = ch_config.as_ref().and_then(|c| c.backup.clone()).context(
                    "bootstrap: --bootstrap-wal-from-archive requires a [backup] \
                             section in --ch-config",
                )?;
                let storage = settings
                    .build_storage()
                    .context("bootstrap: build archive storage")?;
                Some((settings, storage))
            } else {
                None
            };
            let opts = BaseBackupOpts {
                // `basic()` stamp lets `pg_stat_progress_basebackup` and
                // `backup_label` read the label as a wall-clock instant
                label: format!("walshadow-bootstrap-{}", Timestamp::now().basic()),
                fast_checkpoint: args.bootstrap_fast_checkpoint,
                no_verify_checksums: false,
                max_rate_kib: args.bootstrap_max_rate_kib,
                wal: hydrate.is_none(),
            };
            (Box::new(DirectSource::new(src_cfg.clone(), opts)), hydrate)
        }
        BootstrapMode::ObjectStore => {
            let settings = ch_config
                    .as_ref()
                    .and_then(|c| c.backup.clone())
                    .context("bootstrap: --bootstrap-mode object_store requires a [backup] section in --ch-config")?;
            let storage = settings
                .build_storage()
                .context("bootstrap: build archive storage")?;
            let resolved =
                bootstrap_marker::resolve_backup(&storage, &plan.backup_name, previous.as_ref())
                    .await?;
            pinned_backup = Some(resolved.clone());
            let mut src = ObjectStoreSource::new(
                settings.clone(),
                storage.clone(),
                resolved,
                args.spill_dir.clone(),
            );
            if let Some(n) = plan.parallelism {
                src = src.with_parallelism(n);
            }
            (Box::new(src), Some((settings, storage)))
        }
        BootstrapMode::Off => unreachable!("dispatch happened in run()"),
    };

    // Tail drain gets a second CatalogMap clone for rfn → descriptor
    // lookups; cheap since `Arc<RelDescriptor>` values stay shared.
    let drain_catalog = catalog_map.clone();
    // Build the toast resolver up front, sharing its counters with the
    // bootstrap tail. The store-toast flag tells the page walk whether to
    // decode pg_toast_* pages.
    // Counters are the daemon's, not this phase's: the streaming pipeline
    // keeps adding to them, so a load's insert cost survives handoff
    let bootstrap_stats = emitter_stats;
    // Leaf-only pool for the bootstrap tail: caps each value (V3) and
    // bounds decoded rows in flight to insert ack; no admission stage
    let shadow_toast = ch_config.as_ref().is_some_and(|c| c.toast.mode.is_shadow());
    // Same list, in the same order, as the live session seats: the instance
    // started below carries into the pump when bootstrap hands off
    let shadow_databases: Vec<String> = match ch_config.as_ref() {
        Some(cfg) if !cfg.databases.is_empty() => cfg.databases.clone(),
        _ => vec![src_cfg.database.clone()],
    };
    // Shadow serves values once bootstrap starts it; its bridge binds then
    // and run() adopts the instance
    let mut running_shadow: Option<OwnedShadow> = None;
    let shadow_toast_bridge = walshadow::toast::shadow_store::LateBridge::default();
    // Shadow starts before backup WAL processing needs value lookups, so the
    // store reads through a cell this frame binds later
    let resolver = match &ch_config {
        Some(cfg) => ToastResolver::for_mode(
            cfg,
            bootstrap_stats.clone(),
            // Backup replay has no pump to sample xid ceilings
            Some(shadow_toast_bridge.clone().into()),
        )
        .map_err(anyhow::Error::msg)?
        .with_budget(walshadow::budget::MemoryBudget::new(
            cfg.resident_payload_max,
        )),
        None => ToastResolver::disabled(),
    };
    let store_toast = resolver.stores_chunks();

    let ch_target = match ch_config {
        Some(emitter_cfg) => {
            let (mapping, resolved) = bootstrap_build_mapping(&emitter_cfg, &drain_catalog, args)
                .await
                .context("bootstrap: build mapping")?;
            // `initial_load = "none"` (table override, else namespace) opts a
            // relation out of the greenfield snapshot: create it + stream CDC,
            // but don't page-walk its existing rows.
            let skip_initial: HashSet<_> = drain_catalog
                .descriptors()
                .filter_map(|d| {
                    let rn = &d.rel_name;
                    let table_mode = emitter_cfg
                        .table_opt_ins
                        .get(rn)
                        .and_then(|r| r.initial_load.as_deref())
                        .or_else(|| emitter_cfg.table_initial_loads.get(rn).map(String::as_str));
                    let none = match table_mode {
                        Some(s) => s.parse::<InitialLoadMode>() == Ok(InitialLoadMode::None),
                        None => {
                            resolved
                                .namespaces
                                .get(rn.namespace.as_ref())
                                .and_then(|n| n.initial_load)
                                == Some(InitialLoadMode::None)
                        }
                    };
                    none.then(|| rn.clone())
                })
                .collect();
            Some((emitter_cfg, mapping, resolved, skip_initial))
        }
        None => None,
    };

    // Decline unmapped relations at `begin` so their pages never decode.
    // Metrics-only (no CH) has no mapping to filter against, so it walks all
    let (tap_filenodes, needs_oracle, routes) = match &ch_target {
        Some((_, mapping, resolved, skip_initial)) => {
            let routed = mapping.snapshot().await;
            let is_routed = |rn: &RelName| routed.contains_key(rn);
            let walked = |rn: &RelName| is_routed(rn) && !skip_initial.contains(rn);
            (
                walshadow::backfill_bootstrap::tap_filenode_set(&drain_catalog, is_routed, walked)
                    .map(Arc::new),
                walshadow::backfill::bootstrap_oracle::needs_oracle(
                    &drain_catalog,
                    &routed,
                    &resolved.column_rules,
                ),
                routed,
            )
        }
        None => (None, false, Default::default()),
    };

    let shadow_toast_rels: ahash::HashSet<(u32, u32)> = if shadow_toast {
        walshadow::toast::shadow_landing::toast_relations(
            sql_client,
            drain_catalog.descriptors().map(Arc::as_ref),
            tap_filenodes.as_deref(),
        )
        .await?
    } else {
        ahash::HashSet::default()
    };

    // Off the backup window: provisioning is an initdb + pg_dump + apply +
    // restart, and doing it after `BASE_BACKUP` opens parks a live backup
    // through all of it — in object-store mode against walrus's 60 s request
    // cap
    let bootstrap_oracle = if needs_oracle {
        let source_conninfo = format!(
            "host={} port={} user={} dbname={} sslmode={}",
            src_cfg.host,
            src_cfg.port,
            src_cfg.user,
            src_cfg.database,
            if src_cfg.sslmode == SslMode::Disable {
                "disable"
            } else {
                "prefer"
            },
        );
        Some(
            walshadow::backfill::bootstrap_oracle::BootstrapOracle::provision(
                args.spill_dir.join("bootstrap_oracle"),
                source_conninfo,
                src_cfg.password.clone(),
                args.bridge_lib_dir.clone(),
                bridge_workers,
                Duration::from_secs(args.shadow_connect_timeout),
            )
            .await
            .context(
                "bootstrap oracle: greenfield needs it to resolve tier-3 types; \
                 refusing to load empty columns",
            )?,
        )
    } else {
        None
    };
    let oracle = bootstrap_oracle.as_ref().map(|o| o.oracle());

    let marker = bootstrap_marker::begin_attempt(&shadow_data_dir, previous, pinned_backup)
        .await
        .context("prepare shadow data dir for bootstrap")?;
    let resume =
        bootstrap_marker::resumable_extraction(&shadow_data_dir, marker.backup_name.as_deref())?;

    // Sample window floor before BASE_BACKUP
    let source_ident = feed
        .identify_system()
        .await
        .context("bootstrap: sample source write head for the window leg")?;
    let source_major = (feed.server_version_num() / 10000) as u32;

    let mut cfg = BootstrapConfig::new(shadow_data_dir.clone()).with_catalog_filenodes(
        catalog_filenodes
            .into_iter()
            .chain(shadow_toast_rels.iter().copied()),
    );
    if let Some(set) = tap_filenodes {
        cfg = cfg.with_tap_filenodes(set);
    }
    let progress = cfg.progress.clone();
    // Only writer of the registry until the status loop starts. Publishing the
    // whole stage group is what makes oracle-versus-ClickHouse attribution
    // answerable during an initial load, rather than page decode alone
    let oracle_stats = oracle.as_ref().map(|o| o.stats.clone()).unwrap_or_default();
    let bridge_stats = bootstrap_oracle
        .as_ref()
        .map(|o| o.bridge_stats())
        .unwrap_or_default();
    // Bootstrap restores one database, so its bridge counts under that name
    let boot_db = DbMetricSources {
        database: src_cfg.database.clone(),
        bridge: [None, Some(bridge_stats.clone())],
        desc_log: None,
        capture: None,
        resolver: None,
        backfiller: None,
    };
    let ticker = tokio_util::task::AbortOnDropHandle::new(tokio::spawn({
        let metrics = metrics.clone();
        let progress = progress.clone();
        let stats = bootstrap_stats.clone();
        let oracle_stats = oracle_stats.clone();
        let attempt = marker.attempts;
        async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            loop {
                tick.tick().await;
                metrics
                    .set(MetricsSnapshot {
                        by_database: vec![boot_db.series()],
                        ..stage_gauges(&StageCounters {
                            emitter: Some(&stats),
                            oracle: [None, Some(&oracle_stats)],
                            bootstrap: Some(&progress),
                            bootstrap_attempt: attempt,
                            uptime_secs: uptime_from.elapsed().as_secs(),
                        })
                    })
                    .await;
            }
        }
    }));
    let (rx, pump) = match &resume {
        Some(done) => {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            let outcome = BootstrapOutcome {
                start: walshadow::backup_source::StartInfo {
                    start_lsn: done.start_lsn,
                    timeline: done.timeline,
                    tablespaces: Vec::new(),
                },
                end: walshadow::backup_source::EndInfo {
                    end_lsn: done.end_lsn,
                    timeline: done.timeline,
                },
                disk: Arc::default(),
                page_walk: Arc::default(),
                pump: cfg.progress.pump.clone(),
            };
            (rx, tokio::spawn(async move { Ok(outcome) }))
        }
        None => spawn_greenfield_bootstrap(cfg, source, catalog_map, store_toast),
    };
    let pump = tokio_util::task::AbortOnDropHandle::new(pump);

    // Overlay window transaction outcomes on backup pg_xact
    let window_patch = Arc::new(std::sync::Mutex::new(PgXactPatch::new()));
    // Metrics-only mode has no pending gate
    let mut pending_gate: Option<PendingGate> = None;
    // Preserve live failure if file fallback also fails
    let mut window_leg_error: Option<anyhow::Error> = None;
    // Only a live leg borrowed the feed
    let mut live_leg_ran = false;
    let window_scratch = args.spill_dir.join("bootstrap_window");
    tokio::fs::remove_dir_all(&window_scratch).await.ok();

    let (shipped, outcome, window) = if let Some(target) = ch_target {
        let (emitter_cfg, mapping, resolved, skip_initial) = target;
        // Route bootstrap rows through the shared insert tail. Bootstrap
        // is the easy case: every row op=Insert at _lsn = start_lsn, no
        // aborts / TRUNCATE / DDL. Keep operator's flush_timeout; tail
        // defaults 0 to its own partial-flush deadline.
        let addr = format!("{}:{}", emitter_cfg.host, emitter_cfg.port);
        let stats = bootstrap_stats.clone();
        // Window leg shares the tail's fatal, so a CH outage stops both
        let fatal = Fatal::new();
        let inserter_pool_size = emitter_cfg.inserter_pool_size;
        let lanes = bootstrap_lanes(inserter_pool_size, plan.lanes);
        let per_lane = lane_inserters(inserter_pool_size, lanes);
        // Per-batcher budgets, and every lane has one: undivided, the real
        // in-flight ceiling is `lanes * byte_budget`
        let lane_cfg = {
            let mut c = emitter_cfg.clone();
            c.row_budget = (c.row_budget / lanes).max(1);
            c.byte_budget = (c.byte_budget / lanes).max(8 << 20);
            c
        };

        // Throwaway watermark: durability proof is `wait_through(K)`, resume
        // LSN is carried via the WAL pipeline's emitter_ack seed (see `run`),
        // so uniform `commit_lsn = start_lsn` here is fine.
        let mut tails = Vec::with_capacity(lanes);
        for inserters in &per_lane {
            tails.push(
                OwnedTail::spawn(
                    &lane_cfg,
                    *inserters,
                    stats.clone(),
                    fatal.clone(),
                    None,
                    oracle.clone(),
                    "bootstrap",
                )
                .await
                .map_err(anyhow::Error::msg)?,
            );
        }
        tracing::info!(
            target: "walshadow::bootstrap",
            addr = %addr,
            lanes,
            inserters = per_lane.iter().sum::<usize>(),
            row_budget = lane_cfg.row_budget,
            byte_budget = lane_cfg.byte_budget,
            "bootstrap insert tail started",
        );

        // Run WAL window beside page walk when user relations exist
        let mut window_cfg = (!drain_catalog.is_empty()).then(|| {
            walshadow::backfill::bootstrap_window::WindowLegConfig {
                emitter: emitter_cfg.clone(),
                mapping: mapping.clone(),
                config: resolved.clone(),
                stats: stats.clone(),
                resolver: resolver.clone(),
                oracle: oracle.clone(),
                fatal: fatal.clone(),
                scratch_dir: window_scratch.clone(),
                patch: window_patch.clone(),
                catalog: drain_catalog.clone(),
                pg_major: source_major,
                system_id: source_ident.sysid.clone(),
                timeline: source_ident.timeline,
                wind_down: Duration::from_secs(args.bootstrap_wind_down_secs),
            }
        });
        let live_cfg = window_cfg.clone().filter(|_| plan.live_window_leg(args));

        // No source-PG overlay during greenfield bootstrap, and the same
        // mapping snapshot the CREATEs above rendered from: per-relation
        // system column names have to match what CH now holds
        let sink = GreenfieldSink {
            catalog: drain_catalog.clone(),
            mapping: routes,
            config: resolved.clone(),
            emitter: emitter_cfg.clone(),
            stats: stats.clone(),
            resolver: resolver.clone(),
            skip_initial,
            scratch_dir: args.spill_dir.clone(),
            inherit_spools: resume
                .as_ref()
                .map(|done| done.handback_spools.clone())
                .unwrap_or_default(),
        };

        // Gate page tuples, defer unknowns until transaction logs land
        let lane_tails: Vec<_> = tails
            .iter()
            .map(|t| (t.msg_tx.clone(), t.ack.clone()))
            .collect();
        let (drain_txs, stages) = sink
            .spawn(lane_tails, "bootstrap_drain")
            .await
            .map_err(anyhow::Error::msg)?;
        let mut gate_txs = Vec::with_capacity(lanes);
        let mut gate_handles = Vec::with_capacity(lanes);
        for (i, drain_tx) in drain_txs.into_iter().enumerate() {
            let (gate_tx, mut gate_rx) = tokio::sync::mpsc::channel(
                walshadow::backup_page_walk::BOOTSTRAP_TUPLE_CHANNEL_CAP,
            );
            gate_txs.push(gate_tx);
            let spool_path = args
                .spill_dir
                .join(format!("bootstrap_gate_deferred.{i}.bin"));
            // A resumed attempt inherits the spool the extraction checkpoint
            // fsynced; only a fresh pass may clear a stale one
            let inherit = resume.as_ref().and_then(|done| {
                walshadow::bootstrap_marker::SpooledRecords::expected(
                    &done.deferred_spools,
                    &spool_path,
                )
            });
            if inherit.is_none() {
                tokio::fs::remove_file(&spool_path).await.ok();
            }
            let catalog = drain_catalog.clone();
            gate_handles.push(tokio_util::task::AbortOnDropHandle::new(tokio::spawn(
                async move {
                    let mut spool = match inherit {
                        Some(records) => walshadow::spool::DeferredSpool::reopen(
                            spool_path,
                            walshadow::spool::DEFERRED_SPOOL_MEM_MAX,
                            records,
                        )
                        .await
                        .map_err(|e| format!("bootstrap: reopen deferred spool: {e}"))?,
                        None => walshadow::spool::DeferredSpool::new(
                            spool_path,
                            walshadow::spool::DEFERRED_SPOOL_MEM_MAX,
                        ),
                    };
                    let mut gate_stats = GateStats::default();
                    stream_phase(
                        &mut gate_rx,
                        &drain_tx,
                        &catalog,
                        &mut spool,
                        &mut gate_stats,
                        None,
                    )
                    .await
                    .map(|()| (gate_stats, spool))
                },
            )));
        }
        let gate = async move {
            let mut rx = rx;
            let mut at = 0usize;
            while let Some(slab) = rx.recv().await {
                let lane = at % gate_txs.len();
                at += 1;
                if gate_txs[lane].send(slab).await.is_err() {
                    break;
                }
            }
            drop(gate_txs);
            let mut stats = GateStats::default();
            let mut spools = Vec::with_capacity(gate_handles.len());
            for h in gate_handles {
                let (s, spool) = h
                    .await
                    .map_err(|e| format!("bootstrap gate join: {e}"))?
                    .map_err(|e| format!("bootstrap gate: {e}"))?;
                stats.emitted += s.emitted;
                stats.gated += s.gated;
                stats.deferred += s.deferred;
                stats.multixact_emitted += s.multixact_emitted;
                stats.chunks_gated += s.chunks_gated;
                spools.push(spool);
            }
            Ok::<_, String>((stats, spools))
        };

        // Borrow feed until stop watch publishes end_lsn or zero
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(None);
        // Keep sender alive while leg winds down
        let stop_tx = &stop_tx;
        let pump_then_stop = async move {
            let res = pump.await;
            let end = match &res {
                Ok(Ok(o)) => o.end.end_lsn,
                _ => 0,
            };
            let _ = stop_tx.send(Some(end));
            res
        };
        let leg_fut = async {
            match live_cfg {
                Some(cfg) => walshadow::backfill::bootstrap_window::stream_window(
                    cfg,
                    feed,
                    source_ident.xlogpos,
                    stop_rx,
                )
                .await
                .map(Some),
                None => Ok(None),
            }
        };
        let (gate_res, stage_res, pump_res, leg_res) =
            tokio::join!(gate, stages.join(), pump_then_stop, leg_fut);
        let prepared = (|| -> Result<_> {
            let drain_outcome = stage_res.map_err(|e| anyhow::anyhow!(e))?;
            let (gate_stats, gate_spool) =
                gate_res.map_err(|e| anyhow::anyhow!("bootstrap gate: {e}"))?;
            let outcome: BootstrapOutcome = pump_res
                .context("bootstrap pump join")?
                .context("bootstrap pump")?;
            // Retry failed live read from landed WAL
            let window = match leg_res {
                Ok(w) => {
                    live_leg_ran = w.is_some();
                    if live_leg_ran {
                        window_cfg = None;
                    }
                    w
                }
                Err(e) => {
                    tracing::warn!(
                        target: "walshadow::bootstrap",
                        error = %format!("{e:#}"),
                        "live backup-window leg failed; replaying the window from the \
                         WAL the backup landed",
                    );
                    window_leg_error = Some(e);
                    None
                }
            };
            Ok((gate_stats, gate_spool, drain_outcome, outcome, window))
        })();
        let (gate_stats, mut gate_spool, mut drain_outcome, outcome, mut window) = match prepared {
            Ok(prepared) => prepared,
            Err(e) => {
                for tail in tails {
                    tail.quiesce().await;
                }
                return Err(fatal.message().map(anyhow::Error::msg).unwrap_or(e));
            }
        };

        // Only an object-store attempt can re-read the identical backup, so
        // only it can resume past extraction
        if let Some(backup_name) = marker.backup_name.clone().filter(|_| resume.is_none()) {
            for (tail, drained) in tails.iter().zip(&drain_outcome) {
                tail.checkpoint(drained.next_seq)
                    .await
                    .map_err(anyhow::Error::msg)?;
            }
            let mut deferred_spools = Vec::with_capacity(gate_spool.len());
            for spool in &mut gate_spool {
                spool
                    .checkpoint()
                    .await
                    .context("bootstrap: persist deferred gate spool")?;
                if spool.records() > 0 {
                    deferred_spools.push(bootstrap_marker::SpooledRecords {
                        path: spool.path().to_path_buf(),
                        records: spool.records(),
                    });
                }
            }
            let mut handback_spools = Vec::with_capacity(drain_outcome.len());
            for drained in &mut drain_outcome {
                if let Some(spool) = drained.deferred.as_mut() {
                    spool
                        .checkpoint()
                        .await
                        .context("bootstrap: persist deferred referrer spool")?;
                    if spool.records() > 0 {
                        handback_spools.push(bootstrap_marker::SpooledRecords {
                            path: spool.path().to_path_buf(),
                            records: spool.records(),
                        });
                    }
                }
            }
            bootstrap_marker::ExtractedCheckpoint {
                backup_name,
                start_lsn: outcome.start.start_lsn,
                end_lsn: outcome.end.end_lsn,
                timeline: outcome.start.timeline,
                deferred_spools,
                handback_spools,
            }
            .write(&shadow_data_dir)
            .await
            .context("bootstrap: record extraction checkpoint")?;
        }

        if let Some((settings, storage)) = wal_hydrate.take() {
            fetch_wal_into_pg_wal(
                &settings,
                storage,
                &shadow_data_dir,
                outcome.start.start_lsn,
                outcome.end.end_lsn,
                outcome.start.timeline,
            )
            .await
            .context("bootstrap: hydrate shadow pg_wal from object store")?;
        }

        // Preserve original WAL for backup processing before in-place rewrite
        let window_wal = if shadow_toast {
            let dir = args.spill_dir.join("bootstrap_window_wal");
            let copied = walshadow::backfill::wal_landing::copy_window_segments(
                &shadow_data_dir.join("pg_wal"),
                &dir,
                outcome.start.timeline,
                outcome.start.start_lsn,
                outcome.end.end_lsn,
            )
            .await
            .context("bootstrap: copy window WAL for the replay leg")?;
            tracing::info!(
                target: "walshadow::bootstrap",
                segments = copied,
                dir = %dir.display(),
                "copied window WAL so the leg reads it raw",
            );
            dir
        } else {
            shadow_data_dir.join("pg_wal")
        };

        // Rewrite landed WAL before shadow recovery; backup processing uses copy.
        //
        // Non-shadow toast modes rewrite after reading original `pg_wal` below
        if shadow_toast {
            let landed = walshadow::backfill::wal_landing::filter_landed_wal(
                &shadow_data_dir.join("pg_wal"),
                outcome.start.timeline,
                outcome.end.end_lsn,
                landing_tracker.take().expect("landed WAL filtered once"),
                Some((shadow_toast_rels, outcome.start.start_lsn)),
            )
            .await
            .context("bootstrap: filter landed WAL")?;
            tracing::info!(
                target: "walshadow::bootstrap",
                segments = landed.segments,
                segments_blanked = landed.segments_blanked,
                kept = landed.kept,
                dropped = landed.dropped,
                dropped_bytes = landed.dropped_bytes,
                "landed WAL filtered",
            );

            // Start shadow before value reads. Recovery adds values written
            // during backup and repairs torn pages and checksums.
            // PostgreSQL requires data-directory mode 0700 or 0750
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                tokio::fs::set_permissions(&shadow_data_dir, fs::Permissions::from_mode(0o700))
                    .await
                    .with_context(|| {
                        format!("bootstrap: chmod 0700 {}", shadow_data_dir.display())
                    })?;
            }
            // Guard before start, so a failed bootstrap stops what it started
            let started = running_shadow.insert(OwnedShadow::new(
                build_owned_shadow(
                    args,
                    &src_cfg.database,
                    &shadow_databases,
                    shadow_data_dir.clone(),
                    bridge_workers,
                ),
                args.keep_shadow_running,
            ));
            started
                .shadow
                .write_standby_signal()
                .context("bootstrap: write standby.signal")?;
            // Recover to `end_lsn` from local pg_wal
            walshadow::ops::stages::SHADOW_REPLAY
                .measure(start_owned_shadow(
                    &started.shadow,
                    Some(outcome.end.end_lsn),
                    Duration::from_secs(args.bootstrap_shadow_replay_timeout),
                    false,
                ))
                .await
                .context("bootstrap: start shadow to serve TOAST values")?;
            let bridge = walshadow::bridge::connect_with_budget(
                &args.bridge_socket_path(),
                bridge_workers,
                Duration::from_secs(args.shadow_connect_timeout),
            )
            .await
            .context("bootstrap: dial shadow bridge for TOAST values")?;
            shadow_toast_bridge
                .set(Arc::new(bridge))
                .ok()
                .context("bootstrap: shadow TOAST bridge bound twice")?;
        }

        // Replay backup WAL before resolving deferred value references
        if let Some(mut cfg) = window_cfg {
            cfg.timeline = outcome.start.timeline;
            let replayed = async {
                let segments = walshadow::backfill::bootstrap_window::segments_in_dir(
                    &window_wal,
                    outcome.start.timeline,
                    outcome.start.start_lsn,
                    outcome.end.end_lsn,
                )
                .await?;
                walshadow::backfill::bootstrap_window::replay_segments(
                    cfg,
                    &segments,
                    outcome.start.start_lsn,
                    outcome.end.end_lsn,
                )
                .await
            }
            .await;
            match replayed {
                Ok(w) => window = Some(w),
                Err(e) => {
                    for tail in tails {
                        tail.quiesce().await;
                    }
                    let e = match &window_leg_error {
                        Some(live) => {
                            e.context(format!("after the live window leg failed: {live:#}"))
                        }
                        None => e.context("bootstrap: backup-window WAL leg"),
                    };
                    return Err(fatal.message().map(anyhow::Error::msg).unwrap_or(e));
                }
            }
        }

        // Referrers the lanes handed back. One lane reaching its end proves
        // nothing about a sibling's chunk puts, so resolution waits for all
        // of them, then replays each spool on the tail that drained it
        let mut handback = Vec::with_capacity(lanes);
        let mut handback_at = Vec::with_capacity(lanes);
        for (i, outcome) in drain_outcome.iter_mut().enumerate() {
            let Some(spool) = outcome.deferred.take() else {
                continue;
            };
            handback.push(DeferredLane {
                spool,
                msg_tx: tails[i].msg_tx.clone(),
                ack: tails[i].ack.clone(),
                first_seq: outcome.next_seq,
            });
            handback_at.push(i);
        }
        let mut deferred_rows = 0;
        if !handback.is_empty() {
            match sink.resolve_deferred(handback).await {
                Ok(resolved) => {
                    for (i, lane) in handback_at.into_iter().zip(resolved) {
                        drain_outcome[i].next_seq = lane.next_seq;
                        deferred_rows += lane.rows_routed;
                    }
                }
                Err(e) => {
                    for tail in tails {
                        tail.quiesce().await;
                    }
                    return Err(fatal
                        .message()
                        .map(anyhow::Error::msg)
                        .unwrap_or_else(|| anyhow::anyhow!(e)));
                }
            }
        }
        // Seqs are per-lane, so each tail is proven through its own count
        let rows_routed: u64 =
            drain_outcome.iter().map(|d| d.rows_routed).sum::<u64>() + deferred_rows;
        let seqs: u64 = drain_outcome.iter().map(|d| d.next_seq).sum();
        for (tail, outcome) in tails.into_iter().zip(&drain_outcome) {
            tail.finish(outcome.next_seq)
                .await
                .map_err(anyhow::Error::msg)?;
        }
        tracing::info!(
            target: "walshadow::bootstrap",
            rows_routed,
            deferred_rows,
            rows_emitted = stats.rows_emitted.load(Ordering::Relaxed),
            blocks_sent = stats.blocks_sent.load(Ordering::Relaxed),
            seqs,
            "bootstrap insert tail drained",
        );
        pending_gate = Some(PendingGate {
            deferred: gate_spool,
            sink,
            oracle: oracle.clone(),
            stream_stats: gate_stats,
        });
        (rows_routed, outcome, window)
    } else {
        // Metrics-only skips destination convergence
        let mut observer = MetricsTupleObserver::default();
        let (drain_res, pump_res) = tokio::join!(drain_backfill(rx, &mut observer), pump);
        let shipped = drain_res.context("bootstrap drain")?;
        let outcome: BootstrapOutcome = pump_res
            .context("bootstrap pump join")?
            .context("bootstrap pump")?;
        (shipped, outcome, None)
    };

    // Replace live-leg COPY connection and recheck source identity
    if live_leg_ran || window_leg_error.is_some() {
        *feed = SourceFeed::connect(src_cfg)
            .await
            .with_context(|| {
                format!(
                    "bootstrap: reconnect source {}:{} after the window leg",
                    src_cfg.host, src_cfg.port
                )
            })?
            .with_status_interval(Duration::from_secs(args.status_interval));
        let now = feed
            .identify_system()
            .await
            .context("bootstrap: IDENTIFY_SYSTEM after the window leg")?;
        anyhow::ensure!(
            now.sysid == source_ident.sysid && now.timeline == source_ident.timeline,
            "source identity moved during bootstrap: system {} timeline {} when the backup \
             opened, system {} timeline {} now",
            source_ident.sysid,
            source_ident.timeline,
            now.sysid,
            now.timeline,
        );
    }

    tracing::info!(
        target: "walshadow::bootstrap",
        start_lsn = format_pg_lsn(outcome.start.start_lsn).to_string(),
        end_lsn = format_pg_lsn(outcome.end.end_lsn).to_string(),
        timeline = outcome.start.timeline,
        kept_files = outcome.disk.kept_files.load(Ordering::Relaxed),
        skipped_denylist = outcome.disk.skipped_denylist.load(Ordering::Relaxed),
        files_walked = outcome.page_walk.files_walked.load(Ordering::Relaxed),
        tuples_emitted = outcome.page_walk.tuples_emitted.load(Ordering::Relaxed),
        drained = shipped,
        "bootstrap landed",
    );
    // Stage attribution: which of tap, decode or emitter drain owned the
    // wall clock. Sum exceeds elapsed under source parallelism
    tracing::info!(
        target: "walshadow::bootstrap",
        elapsed_secs = timing.elapsed().as_secs_f64(),
        bytes_tapped = outcome.pump.bytes_tapped.load(Ordering::Relaxed),
        pages_walked = outcome.page_walk.pages_walked.load(Ordering::Relaxed),
        tap_secs = outcome.pump.sink_chunk_nanos.load(Ordering::Relaxed) as f64 / 1e9,
        decode_secs = outcome.page_walk.decode_nanos.load(Ordering::Relaxed) as f64 / 1e9,
        channel_block_secs = outcome.page_walk.channel_block_nanos.load(Ordering::Relaxed) as f64 / 1e9,
        files_skipped_unmapped = outcome.page_walk.files_skipped_unmapped.load(Ordering::Relaxed),
        "bootstrap stage timings",
    );
    ticker.abort();

    if let Some((settings, storage)) = wal_hydrate {
        fetch_wal_into_pg_wal(
            &settings,
            storage,
            &shadow_data_dir,
            outcome.start.start_lsn,
            outcome.end.end_lsn,
            outcome.start.timeline,
        )
        .await
        .context("bootstrap: hydrate shadow pg_wal from object store")?;
    }

    let open_floor = window.and_then(|w| w.open_floor);
    if let Some(w) = window {
        tracing::info!(
            target: "walshadow::bootstrap",
            from_lsn = format_pg_lsn(w.from_lsn).to_string(),
            through_lsn = format_pg_lsn(w.through_lsn).to_string(),
            rows = w.replay.rows_replayed,
            commits_below_from = w.replay.commits_below_from,
            unknown_rfns = w.replay.unknown_rfns,
            open_floor = w.open_floor.map(|l| format_pg_lsn(l).to_string()),
            "backup window shipped",
        );
    }
    tokio::fs::remove_dir_all(&window_scratch).await.ok();

    // Non-shadow toast modes rewrite landed WAL after backup processing reads it
    if let Some(tracker) = landing_tracker.take() {
        let landed = walshadow::backfill::wal_landing::filter_landed_wal(
            &shadow_data_dir.join("pg_wal"),
            outcome.start.timeline,
            outcome.end.end_lsn,
            tracker,
            None,
        )
        .await
        .context("bootstrap: filter landed WAL")?;
        tracing::info!(
            target: "walshadow::bootstrap",
            segments = landed.segments,
            segments_blanked = landed.segments_blanked,
            kept = landed.kept,
            dropped = landed.dropped,
            dropped_bytes = landed.dropped_bytes,
            "landed WAL filtered",
        );
    }

    // Resolve deferred tuples after window transaction overlay is complete
    if let Some(pending) = pending_gate {
        let mut patch = std::mem::take(&mut *window_patch.lock().expect("window patch lock"));
        patch.seal();
        let (gate, pending_tables) =
            resolve_greenfield(pending, &shadow_data_dir, &patch, source_major)
                .await
                .context("bootstrap: visibility gate")?;
        // Persist the ledger before clearing the marker so pending rows
        // already in ClickHouse can be published after restart
        let mut ledger = walshadow::visibility_pending::PendingLedger::load(
            &args.spill_dir,
            source_ident
                .sysid
                .parse()
                .context("IDENTIFY_SYSTEM sysid")?,
        )
        .await
        .context("bootstrap: load pending visibility ledger")?;
        for m in &pending_tables {
            ledger
                .push(m)
                .await
                .context("bootstrap: persist pending visibility ledger")?;
        }
        tracing::info!(
            target: "walshadow::bootstrap",
            emitted = gate.emitted,
            gated = gate.gated,
            deferred = gate.deferred,
            pending = gate.pending,
            pending_tables = pending_tables.len(),
            multixact_emitted = gate.multixact_emitted,
            chunks_gated = gate.chunks_gated,
            patch_xacts = patch.len(),
            "bootstrap visibility gate settled",
        );
    }

    // PG refuses to start on a data dir whose mode isn't 0700 or 0750.
    // BASE_BACKUP tar carries no entry for the root, so extraction leaves
    // it at the process umask (typically 0755); reassert 0700 before pg_ctl.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = fs::Permissions::from_mode(0o700);
        tokio::fs::set_permissions(&shadow_data_dir, perms)
            .await
            .with_context(|| format!("bootstrap: chmod 0700 {}", shadow_data_dir.display()))?;
    }

    bootstrap_marker::ExtractedCheckpoint::clear(&shadow_data_dir).await?;
    BootstrapMarker::clear(&shadow_data_dir).await?;

    timing.finish();
    Ok((
        BootstrapHandoff {
            end_lsn: outcome.end.end_lsn,
            open_floor,
            shadow: running_shadow,
        },
        BootstrapMetrics {
            progress,
            oracle: oracle_stats,
            bridge: bridge_stats,
        },
    ))
}

/// Registry the bootstrap ticker writes, and the handles it publishes there.
/// Both outlive bootstrap: the daemon's `t0` and the emitter counters the
/// streaming pipeline goes on adding to
pub(crate) struct BootstrapObservers<'a> {
    pub(crate) metrics: &'a MetricsRegistry,
    pub(crate) emitter_stats: Arc<EmitterStats>,
    pub(crate) uptime_from: Instant,
}

/// What bootstrap leaves behind for the status loop to keep publishing. The
/// oracle handles outlive their throwaway PG, so its request cost stays on
/// the same series the live bridge then adds to
pub(crate) struct BootstrapMetrics {
    pub(crate) progress: BootstrapProgress,
    pub(crate) oracle: Arc<walshadow::oracle::OracleStats>,
    pub(crate) bridge: Arc<walshadow::bridge::BridgeStats>,
}

/// Bootstrap-to-pump handoff
pub(crate) struct BootstrapHandoff {
    /// Backup end and shadow state boundary
    pub(crate) end_lsn: u64,
    /// Earliest record among transactions open at window seal
    pub(crate) open_floor: Option<u64>,
    /// Shadow instance started during bootstrap
    pub(crate) shadow: Option<OwnedShadow>,
}

impl BootstrapHandoff {
    /// Source or archive must retain crossing transaction records
    pub(crate) fn resume_lsn(&self) -> u64 {
        self.open_floor.unwrap_or(self.end_lsn).min(self.end_lsn)
    }
}

/// Routing map for the bootstrap drain: explicit `[table.*]` seeded up front,
/// then every seeded relation run through the DDL applicator's `Added` path so
/// `auto_create` namespaces get their CH table created and mapping registered.
/// Returns the snapshot those CREATEs rendered from, so the drain freezes
/// routes against the same per-relation rules.
pub(crate) async fn bootstrap_build_mapping(
    emitter_cfg: &EmitterConfig,
    catalog: &walshadow::backup_page_walk::CatalogMap,
    args: &Args,
) -> Result<(MappingHandle, Arc<walshadow::config::ResolvedConfig>)> {
    let mapping = walshadow::mapping::mapping_handle(emitter_cfg.tables.clone());
    let cli_overrides = CliOverrides {
        drop_table_strategy: args.drop_table_strategy,
        flush_timeout: args
            .ch_flush_timeout_ms
            .map(std::time::Duration::from_millis),
        source_slot: args.slot.clone(),
    };
    let (_resolver, config_rx) = ConfigResolver::new(
        emitter_cfg,
        cli_overrides,
        args.ch_config.clone(),
        cli_base(args),
        mapping.clone(),
    );
    let (ddl_cfg, merged_tables, resolved) = {
        let snap = config_rx.borrow();
        (
            walshadow::ch_ddl::DdlConfig::from_resolved(
                &snap,
                emitter_cfg.database.clone(),
                emitter_cfg.soft_delete,
                emitter_cfg.system_columns.clone(),
                emitter_cfg.replicate_all,
                emitter_cfg.runtime_config_schema.clone(),
            ),
            Arc::new(snap.tables.clone()),
            snap.clone(),
        )
    };
    // Publish rule-adjusted targets before creating tables
    mapping.publish(merged_tables).await;
    let mut applicator =
        walshadow::ch_ddl::DdlApplicator::new(emitter_cfg, ddl_cfg, mapping.clone(), config_rx)
            .await
            .context("bootstrap: init DDL applicator")?;
    for desc in catalog.descriptors() {
        applicator
            .apply(&SchemaEvent::Added { desc: desc.clone() })
            .await
            .with_context(|| format!("bootstrap: ensure CH table {}", desc.rel_name))?;
    }
    Ok((mapping, resolved))
}

/// Choose one-time bootstrap or resume from `--bootstrap-shadow-data-dir`
/// and data dir state
/// Mode only chooses bootstrap source
pub(crate) enum ShadowStart {
    Bootstrap(PathBuf),
    Rebootstrap(PathBuf, BootstrapMarker),
    Resume(PathBuf),
}

impl ShadowStart {
    pub(crate) fn bootstraps(&self) -> bool {
        matches!(self, Self::Bootstrap(_) | Self::Rebootstrap(..))
    }

    pub(crate) fn data_dir(&self) -> &Path {
        match self {
            Self::Bootstrap(d) | Self::Rebootstrap(d, _) | Self::Resume(d) => d,
        }
    }
}

pub(crate) fn resolve_shadow_start(args: &Args, mode: BootstrapMode) -> Result<ShadowStart> {
    let dir = &args.bootstrap_shadow_data_dir;
    for (flag, other) in [
        ("--out-dir", &args.out_dir),
        ("--spill-dir", &args.spill_dir),
        ("--shadow-socket-dir", &args.shadow_socket_dir),
    ] {
        anyhow::ensure!(
            !paths_overlap(dir, other),
            "--bootstrap-shadow-data-dir {} overlaps {flag} {}",
            dir.display(),
            other.display(),
        );
    }
    if let Some(marker) = bootstrap_marker::pending_attempt(dir, mode)? {
        return Ok(ShadowStart::Rebootstrap(dir.clone(), marker));
    }
    if dir.join("PG_VERSION").exists() {
        if !matches!(mode, BootstrapMode::Off) {
            tracing::info!(
                target: "walshadow::bootstrap",
                data_dir = %dir.display(),
                "shadow data dir already initialized, resuming without bootstrap",
            );
        }
        return Ok(ShadowStart::Resume(dir.clone()));
    }
    anyhow::ensure!(
        !matches!(mode, BootstrapMode::Off),
        "shadow data dir {} does not contain an initialized cluster; bootstrap mode off cannot \
         bootstrap it, pass direct or object_store via --bootstrap-mode or [bootstrap] mode",
        dir.display(),
    );
    Ok(ShadowStart::Bootstrap(dir.clone()))
}

/// True if `a` and `b` are the same path, or one is an ancestor of the other
pub(crate) fn paths_overlap(a: &Path, b: &Path) -> bool {
    match (std::path::absolute(a), std::path::absolute(b)) {
        (Ok(a), Ok(b)) => a == b || a.starts_with(&b) || b.starts_with(&a),
        _ => true,
    }
}

/// Inserter count is the demand on the oracle: one bridge worker and one
/// resolver per inserter is what keeps a batch resolving while the others
/// insert
/// Bounded by the inserter pool: a lane without an inserter cannot insert
pub(crate) fn bootstrap_lanes(inserter_pool_size: usize, override_lanes: Option<usize>) -> usize {
    let ceiling = inserter_pool_size.max(1);
    let want = override_lanes.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    });
    want.clamp(1, ceiling)
}

/// Distributes the remainder; `div_ceil` per lane would hand out more
/// connections than the pool names
pub(crate) fn lane_inserters(inserter_pool_size: usize, lanes: usize) -> Vec<usize> {
    let (base, rem) = (inserter_pool_size / lanes, inserter_pool_size % lanes);
    (0..lanes)
        .map(|i| base + usize::from(i < rem))
        .map(|n| n.max(1))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::args::args_from;

    fn shadow_start(args: &Args) -> Result<ShadowStart> {
        resolve_shadow_start(args, resolve_bootstrap(args, None)?.mode)
    }

    #[test]
    fn bootstrap_lanes_layers_override_over_the_derived_default() {
        let derived = bootstrap_lanes(8, None);
        assert!((1..=8).contains(&derived), "derived {derived}");
        assert_eq!(bootstrap_lanes(8, Some(2)), 2, "override wins");
        assert_eq!(bootstrap_lanes(8, Some(0)), 1, "zero clamps to one lane");
        assert_eq!(bootstrap_lanes(1, None), 1, "a single inserter is one lane");
        assert_eq!(
            bootstrap_lanes(3, Some(16)),
            3,
            "lanes never exceed inserters; a lane with none cannot insert",
        );
    }

    #[test]
    fn lane_inserters_distribute_the_pool_without_overshooting() {
        for (pool, lanes) in [(8, 3), (8, 8), (8, 1), (5, 4), (3, 3), (12, 5)] {
            let split = lane_inserters(pool, lanes);
            assert_eq!(split.len(), lanes, "pool {pool} lanes {lanes}");
            assert!(
                split.iter().all(|n| *n >= 1),
                "every lane needs an inserter"
            );
            assert_eq!(
                split.iter().sum::<usize>(),
                pool,
                "pool {pool} over {lanes} lanes must sum to the pool, got {split:?}",
            );
        }
    }

    #[test]
    fn bootstrap_plan_layers_cli_over_toml() {
        let toml = |s: &str| EmitterConfig::from_toml_str(s).unwrap();

        let cfg = toml(
            "[ch]\n[bootstrap]\nmode = \"object_store\"\nbackup_name = \"base_0000000100000000000000AA\"\nobject_store_parallelism = 8\n",
        );
        let plan = resolve_bootstrap(&args_from(&[]), Some(&cfg)).unwrap();
        assert_eq!(plan.mode, BootstrapMode::ObjectStore);
        assert_eq!(plan.backup_name, "base_0000000100000000000000AA");
        assert_eq!(plan.parallelism, Some(8));

        let plan = resolve_bootstrap(
            &args_from(&[
                "--bootstrap-mode",
                "direct",
                "--bootstrap-backup-name",
                "LATEST",
            ]),
            Some(&cfg),
        )
        .unwrap();
        assert_eq!(plan.mode, BootstrapMode::Direct);
        assert_eq!(plan.backup_name, "LATEST");
        assert_eq!(plan.parallelism, Some(8), "TOML fills what the CLI omits");

        let plan = resolve_bootstrap(&args_from(&[]), Some(&toml("[ch]\n"))).unwrap();
        assert_eq!(plan.mode, BootstrapMode::Direct);
        assert_eq!(plan.backup_name, "LATEST");
        assert_eq!(plan.parallelism, None);

        assert!(
            EmitterConfig::from_toml_str("[ch]\n[bootstrap]\nmode = \"objectstore\"\n").is_err()
        );
        assert!(
            EmitterConfig::from_toml_str("[ch]\n[bootstrap]\nobject_store_parallelism = 0\n")
                .is_err()
        );
    }

    #[test]
    fn bootstrap_mode_accepts_both_object_store_spellings() {
        for spelling in ["object_store", "object-store"] {
            let plan =
                resolve_bootstrap(&args_from(&["--bootstrap-mode", spelling]), None).unwrap();
            assert_eq!(plan.mode, BootstrapMode::ObjectStore, "{spelling}");
        }
    }

    #[test]
    fn shadow_start_bootstrap_vs_resume_keys_on_dir_state() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("data");
        std::fs::create_dir_all(&dir).unwrap();
        let dir_str = dir.to_str().unwrap();
        let direct = |d: &str| {
            args_from(&[
                "--bootstrap-mode",
                "direct",
                "--bootstrap-shadow-data-dir",
                d,
                "--walsender-bind",
                "127.0.0.1:5555",
            ])
        };
        let off = |d: &str| {
            args_from(&[
                "--bootstrap-mode",
                "off",
                "--bootstrap-shadow-data-dir",
                d,
                "--walsender-bind",
                "127.0.0.1:5555",
            ])
        };

        // Direct bootstraps empty dir, off rejects it
        assert!(matches!(
            shadow_start(&direct(dir_str)).unwrap(),
            ShadowStart::Bootstrap(_)
        ));
        assert!(shadow_start(&off(dir_str)).is_err());

        // Resume initialized dir regardless of mode
        std::fs::write(dir.join("PG_VERSION"), b"17\n").unwrap();
        assert!(matches!(
            shadow_start(&direct(dir_str)).unwrap(),
            ShadowStart::Resume(_)
        ));
        assert!(matches!(
            shadow_start(&off(dir_str)).unwrap(),
            ShadowStart::Resume(_)
        ));

        // Incomplete bootstrap: object_store re-extracts itself, the rest
        // still want an operator
        std::fs::write(
            dir.join(walshadow::bootstrap_marker::MARKER_FILENAME),
            b"attempts = 1\nbackup_name = \"base_original\"\n",
        )
        .unwrap();
        assert!(shadow_start(&direct(dir_str)).is_err());
        assert!(shadow_start(&off(dir_str)).is_err());
        assert!(matches!(
            shadow_start(&args_from(&[
                "--bootstrap-mode",
                "object_store",
                "--bootstrap-shadow-data-dir",
                dir_str,
                "--walsender-bind",
                "127.0.0.1:5999",
            ]))
            .unwrap(),
            ShadowStart::Rebootstrap(..)
        ));
        assert!(dir.join("PG_VERSION").exists());
    }

    /// Every mode refuses a marker it cannot act on; only a pinned,
    /// unexhausted `object_store` attempt retries itself
    #[test]
    fn shadow_start_rejects_invalid_markers_on_initialized_directory() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("PG_VERSION"), b"17\n").unwrap();
        for raw in [b"".as_slice(), b"attempts = 1\n", &[0xff]] {
            std::fs::write(tmp.path().join(bootstrap_marker::MARKER_FILENAME), raw).unwrap();
            for mode in ["off", "direct", "object_store"] {
                let args = args_from(&[
                    "--bootstrap-mode",
                    mode,
                    "--bootstrap-shadow-data-dir",
                    tmp.path().to_str().unwrap(),
                    "--walsender-bind",
                    "127.0.0.1:5999",
                ]);
                assert!(shadow_start(&args).is_err());
            }
        }
    }

    #[test]
    fn bootstrap_handoff_preserves_required_history() {
        let crossing = BootstrapHandoff {
            end_lsn: 0x3000,
            open_floor: Some(0x1000),
            shadow: None,
        };
        assert_eq!(crossing.resume_lsn(), 0x1000);

        let clean = BootstrapHandoff {
            end_lsn: 0x3000,
            open_floor: None,
            shadow: None,
        };
        assert_eq!(clean.resume_lsn(), 0x3000);
        assert_eq!(
            BootstrapHandoff {
                end_lsn: 0x3000,
                open_floor: Some(0x4000),
                shadow: None,
            }
            .resume_lsn(),
            0x3000
        );
    }
}
