//! Per-source-database wiring: descriptor log, catalog capture, TOAST
//! resolver, and the decode chain each database owns.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use ahash::HashSet;
use anyhow::Context;
use tokio::sync::Mutex;
use walrus::pg::backup::format_pg_lsn;
use walshadow::ch_emitter::{EmitterConfig, EmitterStats};
use walshadow::config::{CliOverrides, ConfigResolver, SourceConn};
use walshadow::pos::{Floor, Pos};
use walshadow::runtime_config::InitialLoadMode;
use walshadow::schema::{RelName, SchemaEvent};
use walshadow::shadow_catalog::ShadowCatalog;
use walshadow::source_db::{DbLink, SourceDb};

use crate::args::{Args, cli_base};
use crate::runtime_cfg::{apply_toml_initial_loads, seed_runtime_config, spawn_mapping_refresher};

pub(crate) struct SourceDbInputs<'a> {
    pub(crate) args: &'a Args,
    pub(crate) conn: &'a DbLink,
    /// `[source] dbname`: the one database a cluster backup can load
    pub(crate) primary: bool,
    /// Destinations every followed database has claimed
    pub(crate) targets: &'a Arc<walshadow::mapping::TargetOwners>,
    pub(crate) desc_log: &'a Arc<walshadow::desc_log::DescriptorLog>,
    /// Backfill ledger lives here, beside this database's descriptor log
    pub(crate) spill_dir: PathBuf,
    pub(crate) source: &'a SourceConn,
    pub(crate) oracle: &'a Option<Arc<walshadow::oracle::Oracle>>,
    pub(crate) history_rx:
        tokio::sync::watch::Receiver<Arc<walshadow::source::timeline::TimelineHistory>>,
    pub(crate) budget: &'a walshadow::budget::MemoryBudget,
    pub(crate) stats: &'a Arc<EmitterStats>,
    pub(crate) source_major: u32,
    pub(crate) raw_start: Pos<Floor>,
    /// Relations shadow can serve TOAST for, absent unless `[toast] mode = shadow`
    pub(crate) shadow_toast_held: Option<&'a walshadow::filter::shadow_relations::ShadowHeld>,
}

pub(crate) struct BuiltSourceDb {
    pub(crate) db: Arc<SourceDb>,
    pub(crate) applicator: Option<walshadow::ch_ddl::DdlApplicator>,
    pub(crate) backfiller: Option<Arc<walshadow::copy_backfill::CopyBackfiller>>,
}

/// Wire one database's routing map, config resolver, DDL applicator and
/// backfiller, then re-materialise its opt-in scope
pub(crate) async fn build_source_db(input: SourceDbInputs<'_>) -> anyhow::Result<BuiltSourceDb> {
    let args = input.args;
    let conn = input.conn;
    let desc_log = input.desc_log;
    let raw_start = input.raw_start;
    let Some(mut cfg) = conn.emitter.clone() else {
        return Ok(BuiltSourceDb {
            db: Arc::new(metrics_only_db(conn, desc_log)),
            applicator: None,
            backfiller: None,
        });
    };
    // Live routing map shared by DDL applicator + route planning. The
    // refresher below rewrites it on every republished snapshot.
    let mapping = walshadow::mapping::mapping_handle(cfg.tables.clone());
    // Resolver merges CLI over TOML and publishes ResolvedConfig on
    // the watch substrate; SIGHUP re-reads TOML and republishes. The
    // mapping refresher + DDL applicator subscribe.
    let cli_overrides = CliOverrides {
        drop_table_strategy: args.drop_table_strategy,
        flush_timeout: args
            .ch_flush_timeout_ms
            .map(std::time::Duration::from_millis),
        source_slot: args.slot.clone(),
    };
    let (resolver, config_rx) = ConfigResolver::new(
        &cfg,
        cli_overrides,
        args.ch_config.clone(),
        cli_base(args),
        mapping.clone(),
    );
    if let Some(held) = input.shadow_toast_held {
        resolver.bind_shadow_toast(held.clone());
        // Check configured tables here because they bypass opt-in
        // Preserve exclusions across SIGHUP reloads
        let descs = conn
            .catalog
            .lock()
            .await
            .descriptors_by_name(cfg.tables.keys())
            .await?;
        for rel in
            walshadow::toast::shadow_landing::unserved_rels(&conn.catalog, held, &descs).await?
        {
            resolver.exclude_table(&rel).await;
        }
    }
    spawn_mapping_refresher(config_rx.clone(), mapping.clone());
    // Runtime-config overlay (§7): before the pump consumes WAL, seed the
    // resolver from this database's config_* tables over a sidecar libpq
    // connection. Post-seed writes arrive live off the WAL stream. Refuse
    // to start if the named schema is not installed — explicit opt-in
    // means the operator expects the overlay present.
    let mut seeded_table_rows: Vec<(RelName, walshadow::runtime_config::TableRow)> = Vec::new();
    if let Some(schema) = cfg.runtime_config_schema.clone() {
        let client = open_source_sql_client(input.source, &conn.name)
            .await
            .context("sidecar sql for runtime-config seed")?;
        seeded_table_rows = seed_runtime_config(&client, &schema, &resolver)
            .await
            .context("seed runtime config overlay")?;
    }
    {
        let rc = config_rx.borrow();
        cfg.row_budget = rc.row_budget;
        cfg.byte_budget = rc.byte_budget;
        cfg.flush_timeout = rc.flush_timeout;
        cfg.compression = rc.compression;
        cfg.retry.max_attempts = rc.retry_max_attempts;
    }
    // DDL applicator owned by the reorder coordinator so ALTER /
    // CREATE / DROP / TRUNCATE apply inside the barrier, after
    // earlier data is durable. Seeds DDL config from the resolved
    // snapshot; refreshes per apply as the resolver republishes.
    let ddl_cfg = walshadow::ch_ddl::DdlConfig::from_resolved(
        &config_rx.borrow(),
        cfg.database.clone(),
        cfg.soft_delete,
        cfg.system_columns.clone(),
        cfg.replicate_all,
        cfg.runtime_config_schema.clone(),
    );
    let mut applicator =
        walshadow::ch_ddl::DdlApplicator::new(&cfg, ddl_cfg, mapping.clone(), config_rx.clone())
            .await
            .context("init DDL applicator")?
            .with_resolver(resolver.clone())
            .with_oracle(input.oracle.clone())
            .with_target_owners(conn.oid, input.targets.clone());
    // Backfiller for `initial_load` opt-ins (COPY / backup-sourced):
    // own source session + CH tail per backfill or pass, spill-dir
    // ledger dedups restarts. Wired whenever the emitter runs, since an
    // opt-in arriving later over the control socket or the overlay would
    // otherwise silently skip its backfill; idle it costs one ledger read.
    let mut pg = input.source.to_pg_config();
    pg.database = conn.name.clone();
    let backfiller = Arc::new(
        walshadow::copy_backfill::CopyBackfiller::new(
            pg,
            cfg.clone(),
            mapping.clone(),
            input.stats.clone(),
            conn.catalog.clone(),
            desc_log.clone(),
            &input.spill_dir,
            Some(config_rx.clone()),
            input.history_rx,
            Some(input.budget.clone()),
            input.oracle.clone(),
            input.source_major,
        )
        .await
        .with_backup_loads(input.primary),
    );
    let backfiller_effects: Option<Arc<dyn walshadow::opt_in::Backfiller>> =
        Some(backfiller.clone() as _);
    // Re-materialise per-table opt-in scope from the seeded config_table
    // rows. Live edits arrive off WAL via the reorder coordinator, but a
    // restart replays WAL from past these rows' commit LSN, so the seed
    // is the only chance to rebuild their scope (the CH tables persist).
    // `raw_start` is the backfill boundary S for a first-seen
    // `initial_load` row: COPY covers commits before it, WAL the rest;
    // the ledger resumes/no-ops rows seen on an earlier boot.
    for (rel, row) in &seeded_table_rows {
        if row.replicate.is_some() && !row.is_pattern() {
            walshadow::opt_in::apply_table_opt_in(
                &resolver,
                &mut applicator,
                &conn.catalog,
                backfiller_effects.as_ref(),
                rel,
                row,
                raw_start.get(),
            )
            .await
            .with_context(|| format!("seed opt-in for {rel}"))?;
        }
    }
    for (rel, row) in &cfg.table_opt_ins {
        if row.replicate.is_some() {
            walshadow::opt_in::apply_table_opt_in(
                &resolver,
                &mut applicator,
                &conn.catalog,
                backfiller_effects.as_ref(),
                rel,
                row,
                raw_start.get(),
            )
            .await
            .with_context(|| format!("config opt-in for {rel}"))?;
        }
    }
    let pattern_scoped: Vec<(RelName, walshadow::runtime_config::TableRow)> = {
        let snap = config_rx.borrow();
        let config_schema = cfg.runtime_config_schema.as_deref();
        snap.rules.pattern_scoped(
            || desc_log.user_rel_names_at(raw_start.get(), config_schema),
            |rel| snap.tables.contains_key(rel),
        )
    };
    for (rel, row) in &pattern_scoped {
        walshadow::opt_in::apply_table_opt_in(
            &resolver,
            &mut applicator,
            &conn.catalog,
            backfiller_effects.as_ref(),
            rel,
            row,
            raw_start.get(),
        )
        .await
        .with_context(|| format!("pattern opt-in for {rel}"))?;
    }
    let sql_scoped_tables: HashSet<RelName> = seeded_table_rows
        .iter()
        .filter(|(_, row)| row.replicate.is_some() && !row.is_pattern())
        .chain(pattern_scoped.iter())
        .map(|(rel, _)| rel.clone())
        .collect();
    let active_tables: HashSet<RelName> = config_rx.borrow().tables.keys().cloned().collect();
    apply_toml_initial_loads(
        &conn.catalog,
        Some(&backfiller),
        &cfg.table_initial_loads,
        &active_tables,
        &sql_scoped_tables,
        raw_start.get(),
    )
    .await?;
    // Baseline seeding suppresses the Added event for pinned mappings, so a
    // plain TOML mapping (no initial_load, no opt-in) would tail into a
    // missing CH table. Ensure those dests here; the others own their copy.
    let pinned = active_tables.iter().filter(|rel| {
        let has_initial_load = cfg
            .table_initial_loads
            .get(*rel)
            .and_then(|mode| mode.parse::<InitialLoadMode>().ok())
            .is_some_and(|m| m != InitialLoadMode::None);
        !sql_scoped_tables.contains(*rel) && !has_initial_load
    });
    let descs = conn
        .catalog
        .lock()
        .await
        .descriptors_by_name(pinned)
        .await
        .context("resolve descriptors for pinned mappings")?;
    for desc in descs {
        let rel = desc.rel_name.clone();
        applicator
            .apply(&SchemaEvent::Added {
                desc: Arc::new(desc),
            })
            .await
            .with_context(|| format!("ensure CH dest for pinned mapping {rel}"))?;
    }
    Ok(BuiltSourceDb {
        db: Arc::new(SourceDb::new(
            conn,
            desc_log.clone(),
            cfg,
            mapping,
            Some((resolver, config_rx)),
        )),
        applicator: Some(applicator),
        backfiller: Some(backfiller),
    })
}

/// Without `[ch]` a database routes nothing, so it needs no mapping or resolver
pub(crate) fn metrics_only_db(
    conn: &DbLink,
    desc_log: &Arc<walshadow::desc_log::DescriptorLog>,
) -> SourceDb {
    SourceDb::new(
        conn,
        desc_log.clone(),
        EmitterConfig::default(),
        walshadow::mapping::mapping_handle(Default::default()),
        None,
    )
}

pub(crate) struct DescLogInputs<'a> {
    pub(crate) args: &'a Args,
    /// Spill subdirectory holding this database's log files
    pub(crate) dir: &'a Path,
    pub(crate) dbname: &'a str,
    pub(crate) catalog: &'a Arc<Mutex<ShadowCatalog>>,
    pub(crate) identity: walshadow::desc_log::DescLogIdentity,
    pub(crate) lineage: &'a [u32],
    pub(crate) manifest_present: bool,
    pub(crate) start_lsn_override: Option<Pos<Floor>>,
    pub(crate) raw_start: Pos<Floor>,
    pub(crate) aligned: Pos<Floor>,
}

/// Open one database's descriptor log, seeding a baseline when it is empty
pub(crate) async fn open_db_desc_log(
    input: DescLogInputs<'_>,
) -> anyhow::Result<Arc<walshadow::desc_log::DescriptorLog>> {
    let args = input.args;
    let dir = input.dir;
    // A resumed manifest implies prior progress whose records the log must
    // cover; an empty/missing log there means it was lost — decode would
    // read uncovered intervals. `--ignore-cursor` discards both.
    let log_files_present = dir.join(walshadow::desc_log::TAIL_FILE).exists()
        || dir.join(walshadow::desc_log::CKPT_FILE).exists();
    anyhow::ensure!(
        !input.manifest_present || log_files_present || args.ignore_cursor,
        "manifest present but descriptor log missing in {}; \
         re-bootstrap or pass --ignore-cursor",
        dir.display(),
    );
    if args.ignore_cursor {
        for f in [
            walshadow::desc_log::CKPT_FILE,
            walshadow::desc_log::TAIL_FILE,
        ] {
            let _ = tokio::fs::remove_file(dir.join(f)).await;
        }
    }
    let desc_log = Arc::new(
        walshadow::desc_log::DescriptorLog::open_on_branch(dir, input.identity, input.lineage)
            .await
            .with_context(|| format!("open descriptor log for database {}", input.dbname))?,
    );
    if let Some(lsn) = input.start_lsn_override {
        anyhow::ensure!(
            lsn >= desc_log.floor_at_write(),
            "--start-lsn {} below descriptor log floor {}; no shape history \
             survives there — --ignore-cursor or re-bootstrap",
            lsn,
            desc_log.floor_at_write(),
        );
        let head = desc_log.head();
        anyhow::ensure!(
            head == 0 || lsn.get() <= head,
            "--start-lsn {} beyond descriptor log head {}; boundaries in \
             between were never captured — --ignore-cursor re-baselines",
            lsn,
            format_pg_lsn(head),
        );
    }
    if desc_log.is_empty() {
        // Baseline snapshot: every eligible rel as of shadow's position,
        // valid from the aligned start so the prefix re-read decodes
        // (newest-shape reader of older tuples — the safe bias direction).
        // Boundaries at or below covered_through are baked in and skip.
        let (replay_lsn, descs) = input
            .catalog
            .lock()
            .await
            .fetch_all_descriptors()
            .await
            .with_context(|| format!("descriptor log boot seed for {}", input.dbname))?;
        let covered_through = input.raw_start.get().max(replay_lsn);
        let entries = descs
            .into_iter()
            .map(|d| {
                Arc::new(walshadow::desc_log::LogEntry {
                    valid_from: input.aligned.get(),
                    oid: d.oid,
                    rfn: d.rfn,
                    value: walshadow::desc_log::LogValue::Present(Arc::new(d)),
                })
            })
            .collect();
        desc_log
            .seed(
                walshadow::desc_log::BatchRecord {
                    captured_at: covered_through,
                    commit_lsn: 0,
                    observations: Vec::new(),
                    ambiguities: Vec::new(),
                    entries,
                },
                covered_through,
            )
            .await
            .with_context(|| format!("seed descriptor log for {}", input.dbname))?;
        tracing::info!(
            target: "walshadow::desc_log",
            dbname = %input.dbname,
            covered_through = format_pg_lsn(covered_through).to_string(),
            "descriptor log seeded",
        );
    }
    Ok(desc_log)
}

/// Source sidecar connection to one database, for config seeding and COPY
pub(crate) async fn open_source_sql_client(
    source: &SourceConn,
    dbname: &str,
) -> anyhow::Result<tokio_postgres::Client> {
    let mut cfg = source.to_pg_config();
    cfg.database = dbname.to_string();
    let client = walshadow::source_feed::open_sql_client(&cfg)
        .await
        .with_context(|| format!("open sql client for database {dbname}"))?;
    Ok(client)
}
