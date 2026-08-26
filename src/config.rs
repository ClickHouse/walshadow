//! Layered runtime config resolver. Merges operator config layers into a
//! single pre-materialised [`ResolvedConfig`] and publishes it on a
//! `watch` channel subscribers snapshot from.
//!
//! Precedence, highest wins: **CLI flag > `<schema>.config_*` PG row > TOML**.
//! The PG-row layer (the runtime-config overlay,
//! [plans/future/runtime_config_from_pg.md]) is typed in-memory state
//! ([`crate::runtime_config::ConfigOverlay`]) seeded at boot from source PG and
//! mutated live by [`ConfigResolver::apply_config_event`] as config-table WAL
//! writes drain at their commit LSN. `resolve` is the single merge point.
//!
//! Everything the operator tunes lives on `ResolvedConfig` and reloads live:
//! per-relation mapping, per-namespace defaults, drop-table strategy, the
//! emitter batch/compression/retry knobs, both connections, and the columns-less
//! table opt-ins — all read off the watch channel (pump, batcher, inserter, DDL
//! applicator, reorder coordinator). TOAST and slot creation stay boot-only.
//!
//! **Storage: in-memory.** The overlay is a derived cache — re-seeded from PG
//! then caught up by WAL replay on restart — so it holds no checkpoint. The
//! resolver rebuilds `ResolvedConfig` whole per apply, so a subscriber snapshot
//! never tears.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{Mutex, watch};

use clickhouse_c::{Allocator, TypeAst};
use walrus::pg::replication::conn::PgConfig;
use walrus::pg::replication::tls::{SslMode, TlsParams};

use crate::ch::{CompressionChoice, EmitterError};
use crate::column_rules::{ColumnRule, ColumnRules, ColumnRulesBuilder};
use crate::emit::ch_emitter::EmitterConfig;
use crate::mapping::{
    DropTableStrategy, MappingHandle, NamespaceMapping, TableMapping, TableTarget,
    derive_columns_for_mapping, fold_diff_into_mapping,
};
use crate::runtime_config::{ConfigEvent, ConfigOverlay, TableRow};
use crate::schema::{RelDescriptor, RelName, SchemaDiff};
use crate::table_rules::{MatchKind, TableRules, TableRulesBuilder};
use ahash::{HashMap, HashSet};

#[derive(Clone, PartialEq, Eq)]
pub struct SourceConn {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub dbname: String,
    pub sslmode: SslMode,
    pub slot: Option<String>,
}

impl Default for SourceConn {
    fn default() -> Self {
        Self {
            host: String::new(),
            port: 5432,
            user: String::new(),
            password: None,
            dbname: String::new(),
            // libpq default, matching `PgConfig::resolve`
            sslmode: SslMode::Prefer,
            slot: None,
        }
    }
}

/// Masks the password: `ResolvedConfig` is logged whole in places
impl std::fmt::Debug for SourceConn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceConn")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "***"))
            .field("dbname", &self.dbname)
            .field("sslmode", &self.sslmode)
            .field("slot", &self.slot)
            .finish()
    }
}

impl SourceConn {
    /// Parse `[source]` out of a merged config table. Absent section keeps the
    /// defaults; a malformed port or sslmode is an error so `ctl apply`
    /// rejects it before the daemon reloads onto an unreachable endpoint.
    pub fn from_table(root: &toml::Table) -> Result<Self, String> {
        let mut out = Self::default();
        let Some(src) = root.get("source").and_then(toml::Value::as_table) else {
            return Ok(out);
        };
        if let Some(v) = src.get("host") {
            out.host = scalar_string(v).ok_or("source.host: expected a string")?;
        }
        if let Some(v) = src.get("port") {
            out.port = match v {
                toml::Value::Integer(i) => {
                    u16::try_from(*i).map_err(|_| format!("source.port: {i} out of range"))?
                }
                v => scalar_string(v)
                    .and_then(|s| s.parse().ok())
                    .ok_or("source.port: expected a port number")?,
            };
        }
        if let Some(v) = src.get("user") {
            out.user = scalar_string(v).ok_or("source.user: expected a string")?;
        }
        if let Some(v) = src.get("password") {
            out.password = Some(scalar_string(v).ok_or("source.password: expected a string")?);
        }
        if let Some(v) = src.get("dbname") {
            out.dbname = scalar_string(v).ok_or("source.dbname: expected a string")?;
        }
        if let Some(v) = src.get("sslmode") {
            let s = scalar_string(v).ok_or("source.sslmode: expected a string")?;
            out.sslmode = SslMode::parse(&s).map_err(|e| format!("source.sslmode: {e}"))?;
        }
        if let Some(v) = src.get("slot") {
            let s = scalar_string(v).ok_or("source.slot: expected a string")?;
            out.slot = (!s.is_empty()).then_some(s);
        }
        Ok(out)
    }

    /// Connection for the replication socket, its sidecar SQL client, and
    /// backfill COPY. TLS material comes from the libpq env vars
    /// (`PGSSLROOTCERT` / `PGSSLCERT` / `PGSSLKEY`), never from TOML, so it is
    /// resolved per connect rather than carried here.
    pub fn to_pg_config(&self) -> PgConfig {
        PgConfig {
            host: self.host.clone(),
            port: self.port,
            user: self.user.clone(),
            password: self.password.clone(),
            database: self.dbname.clone(),
            application_name: "walshadow".into(),
            sslmode: self.sslmode,
            tls: TlsParams::resolve(&walrus::config::Vars::default()),
        }
    }

    /// Credential-free endpoint for logs, metrics labels, and `ctl status`
    pub fn endpoint(&self) -> String {
        format!("{}:{}/{}", self.host, self.port, self.dbname)
    }
}

/// TOML scalar as a string. Tables and arrays yield `None`; the CLI `[source]`
/// base layer writes an integer port, files write either.
fn scalar_string(v: &toml::Value) -> Option<String> {
    match v {
        toml::Value::String(s) => Some(s.clone()),
        toml::Value::Table(_) | toml::Value::Array(_) => None,
        v => Some(v.to_string()),
    }
}

/// Pre-materialised resolved config, snapshotted by subscribers via
/// `watch::Receiver<Arc<ResolvedConfig>>`. Rebuilt whole on every reload,
/// so a snapshot is internally consistent — no per-field tearing.
#[derive(Debug, Clone)]
pub struct ResolvedConfig {
    /// Per-relation destination mapping
    pub tables: HashMap<RelName, TableMapping>,
    /// Per-namespace defaults keyed on PG schema name
    pub namespaces: HashMap<String, NamespaceMapping>,
    pub column_rules: Arc<ColumnRules>,
    pub rules: Arc<TableRules>,
    /// Global DROP TABLE strategy fallback (`retain` / `drop` / `warn`);
    /// per-namespace `NamespaceMapping::drop_table_strategy` overrides it
    pub drop_table_strategy: String,
    /// Emitter batch-seal row trigger (live: batcher reads per seal decision)
    pub row_budget: usize,
    /// Emitter batch-seal byte trigger (live)
    pub byte_budget: usize,
    /// Hold-open / idle flush deadline (live)
    pub flush_timeout: Duration,
    /// Per-INSERT wire compression (live: inserter rebuilds its codec on change)
    pub compression: CompressionChoice,
    /// CH client retry budget (live: inserter reads per attempt loop)
    pub retry_max_attempts: u32,
    /// CH connection (live: inserter + DDL applicator reconnect on change).
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    pub secure: bool,
    /// Source PostgreSQL connection and slot, live via config reload
    pub source: SourceConn,
    /// Columns-less `[table.*]` opt-in intents (live: reorder coordinator
    /// applies the add/remove diff at a commit barrier).
    pub table_opt_ins: HashMap<RelName, TableRow>,
    /// `[stream] paused` (live: pump idles when true).
    pub paused: bool,
}

impl ResolvedConfig {
    /// True when `cfg` already dials this destination. Pairs with
    /// [`Self::overlay_dest`] for callers caching the overlay: they rebuild
    /// only once this goes false, so every field written there is read here.
    pub fn dest_conn_eq(&self, cfg: &EmitterConfig) -> bool {
        cfg.host == self.host
            && cfg.port == self.port
            && cfg.database == self.database
            && cfg.user == self.user
            && cfg.password == self.password
            && cfg.secure == self.secure
    }

    /// `base` pointed at the live destination, tuning and mapping untouched.
    /// For sessions that connect eagerly off a boot config — a backfill tail
    /// or staging session — so they take a moved endpoint at spawn instead of
    /// dialling the old address first.
    pub fn overlay_dest(&self, base: &EmitterConfig) -> EmitterConfig {
        EmitterConfig {
            host: self.host.clone(),
            port: self.port,
            database: self.database.clone(),
            user: self.user.clone(),
            password: self.password.clone(),
            secure: self.secure,
            ..base.clone()
        }
    }
}

impl Default for ResolvedConfig {
    fn default() -> Self {
        // Derive from an all-defaults config so budgets are the real defaults,
        // never a 0-budget footgun. Runtime never uses this — the watch channel
        // is seeded from `resolve` — but keeps the type `Default`.
        ConfigResolver::resolve(
            &EmitterConfig::default(),
            &ConfigOverlay::default(),
            &CliOverrides::default(),
            &OptInState::default(),
            &ColumnRules::default(),
        )
        .0
    }
}

/// CLI-layer overrides. `Some` means the operator set the flag explicitly
/// on the command line, so it wins over the overlay + TOML; `None` defers.
/// clap yields `None` for an absent optional flag, so default-vs-explicit
/// falls out of the `Option` with no `value_source` probe. Only knobs with a
/// CLI flag today live here.
#[derive(Debug, Clone, Default)]
pub struct CliOverrides {
    pub drop_table_strategy: Option<String>,
    pub flush_timeout: Option<Duration>,
    pub source_slot: Option<String>,
}

/// CLI value wins over TOML; caller applies any default.
pub fn cli_over_toml<T>(cli: Option<T>, toml: Option<T>) -> Option<T> {
    cli.or(toml)
}

/// Per-table opt-in state derived from `config_table.replicate`, kept
/// alongside the merge inputs. Distinct from the overlay because building an
/// opt-in `TableMapping` needs the source `RelDescriptor` (catalog state the
/// pure [`ConfigResolver::resolve`] merge has no access to); the coordinator /
/// boot path resolves the descriptor and calls
/// [`ConfigResolver::materialize_opt_in`].
///
/// Opt-in mappings live here, not in `base.tables`, so the `MappingHandle`
/// full-swap in [`ConfigResolver::republish`] keeps them — fixing, for opt-in
/// rels, the clobber the base [config.md] "Known limitation" describes.
#[derive(Debug, Clone, Default)]
struct OptInState {
    /// Descriptor-derived mappings for tables opted in via `replicate=true`.
    /// Overlaid onto `resolved.tables`.
    mappings: HashMap<RelName, TableMapping>,
    /// Applicator-derived runtime mappings: `auto_create` CREATEs and ALTER
    /// diff folds ([`ConfigResolver::apply_schema_diff`]). Overlaid onto
    /// `resolved.tables` under `mappings`, so opt-in wins. Living here (not
    /// only in the live handle) is what makes them survive the republish
    /// full-swap — closing the auto-create half of the [config.md] "Known
    /// limitation" clobber.
    derived: HashMap<RelName, TableMapping>,
    /// Tables opted out via `replicate=false` / `TableRemoved`; removed from
    /// `resolved.tables` even when TOML-mapped.
    excluded: HashSet<RelName>,
    /// `replicate=true` rows whose rel isn't known yet (forward-declared),
    /// materialised when the matching `CREATE TABLE` arrives.
    pending_decl: HashMap<RelName, TableRow>,
}

/// The mutable merge inputs, behind one lock so an apply is atomic against a
/// concurrent SIGHUP reload.
struct MergeInputs {
    /// Last-parsed TOML config (layer 3). Replaced by [`ConfigResolver::reload`].
    base: EmitterConfig,
    /// Source-PG overlay (layer 2). Seeded at boot, mutated per WAL config event.
    overlay: ConfigOverlay,
    /// Per-table opt-in state (§per-table opt-in). Merged into `resolved.tables`.
    opt_in: OptInState,
}

/// Owns the `watch::Sender` and the layers it merges. Shared (`Arc`) between
/// the SIGHUP task (calls [`reload`](Self::reload)) and the WAL apply path
/// (calls [`apply_config_event`](Self::apply_config_event)).
pub struct ConfigResolver {
    /// `--ch-config`; `None` disables reload (nothing to re-read)
    toml_path: Option<PathBuf>,
    /// CLI-arg `[source]` base layer, merged under the file on reload (matches
    /// boot's `load_effective`).
    cli_source_base: toml::Table,
    cli: CliOverrides,
    inner: Mutex<MergeInputs>,
    tx: watch::Sender<Arc<ResolvedConfig>>,
    /// Live routing map shared with the decode pool. A WAL config apply writes
    /// it synchronously under the barrier fence (plan §6) so trailing rows in
    /// the applying xact route against the post-config mapping, not waiting on
    /// the async watch refresher.
    mapping: MappingHandle,
    /// Count of overlay values currently rejected at merge (Regime A).
    rejections: AtomicU64,
    /// Forward-declared opt-in rels awaiting their `CREATE TABLE` (gauge).
    pending_decl: AtomicU64,
    /// Cumulative `replicate=true` materialisations / `replicate=false`
    /// exclusions applied.
    opt_in_total: AtomicU64,
    opt_out_total: AtomicU64,
}

impl ConfigResolver {
    /// Build from the boot-parsed [`EmitterConfig`] plus the CLI overlay.
    /// Returns the shared resolver and a receiver seeded with the initial
    /// (overlay-empty) snapshot; call [`seed_overlay`](Self::seed_overlay)
    /// before pump start to fold in the source-PG rows.
    pub fn new(
        base: &EmitterConfig,
        cli: CliOverrides,
        toml_path: Option<PathBuf>,
        cli_source_base: toml::Table,
        mapping: MappingHandle,
    ) -> (Arc<Self>, watch::Receiver<Arc<ResolvedConfig>>) {
        let overlay = ConfigOverlay::default();
        let opt_in = OptInState::default();
        let (initial, _) = Self::resolve(base, &overlay, &cli, &opt_in, &ColumnRules::default());
        let (tx, rx) = watch::channel(Arc::new(initial));
        let this = Arc::new(Self {
            toml_path,
            cli_source_base,
            cli,
            inner: Mutex::new(MergeInputs {
                base: base.clone(),
                overlay,
                opt_in,
            }),
            tx,
            mapping,
            rejections: AtomicU64::new(0),
            pending_decl: AtomicU64::new(0),
            opt_in_total: AtomicU64::new(0),
            opt_out_total: AtomicU64::new(0),
        });
        (this, rx)
    }

    /// Another receiver on the same channel.
    pub fn subscribe(&self) -> watch::Receiver<Arc<ResolvedConfig>> {
        self.tx.subscribe()
    }

    /// Overlay values currently rejected at merge.
    pub fn rejections(&self) -> u64 {
        self.rejections.load(Ordering::Relaxed)
    }

    /// Forward-declared opt-in rels awaiting their `CREATE TABLE`.
    pub fn pending_decl_count(&self) -> u64 {
        self.pending_decl.load(Ordering::Relaxed)
    }

    /// Cumulative `replicate=true` materialisations applied.
    pub fn opt_in_total(&self) -> u64 {
        self.opt_in_total.load(Ordering::Relaxed)
    }

    /// Cumulative `replicate=false` / `TableRemoved` exclusions applied.
    pub fn opt_out_total(&self) -> u64 {
        self.opt_out_total.load(Ordering::Relaxed)
    }

    /// Replace the overlay wholesale (boot `SELECT *` seed, §7) and republish.
    pub async fn seed_overlay(&self, overlay: ConfigOverlay) {
        let mut inner = self.inner.lock().await;
        inner.overlay = overlay;
        self.republish(&inner).await;
    }

    /// Apply one WAL-driven config event at its commit LSN (§6). Mutates the
    /// overlay, writes the routing map under the fence, then republishes.
    /// Called from the reorder coordinator's barrier apply, so it runs after
    /// earlier data in the xact is durable and before the trailing segment
    /// dispatches (decode memoises per job — nothing holds a stale mapping
    /// across the fence).
    pub async fn apply_config_event(&self, event: ConfigEvent) {
        let mut inner = self.inner.lock().await;
        inner.overlay.apply(event);
        self.republish(&inner).await;
    }

    /// Bring `desc` into scope (`replicate=true`, rel known): derive a mapping
    /// from the descriptor, store it so it survives republish, drop any
    /// pending / excluded state, republish. Idempotent —
    /// a re-apply overwrites with an identical mapping. The caller must ensure
    /// the CH table exists first (see `DdlApplicator::ensure_ch_table`).
    pub async fn materialize_opt_in(
        &self,
        desc: &RelDescriptor,
        db_override: Option<String>,
        table_override: Option<String>,
    ) {
        let mut inner = self.inner.lock().await;
        let rel = desc.rel_name.clone();
        // Keep routing target aligned with DDL target
        let settings = self.tx.borrow().rules.settings(&rel);
        let target = TableTarget {
            database: db_override
                .or(settings.target_database)
                .unwrap_or_else(|| Self::target_db_for(&inner, &rel.namespace)),
            table: table_override
                .or(settings.target_table)
                .unwrap_or_else(|| rel.name.to_string()),
        };
        let column_rules = self.tx.borrow().column_rules.clone();
        let columns = derive_columns_for_mapping(desc, &column_rules);
        inner
            .opt_in
            .mappings
            .insert(rel.clone(), TableMapping { target, columns });
        inner.opt_in.derived.remove(&rel);
        inner.opt_in.excluded.remove(&rel);
        inner.opt_in.pending_decl.remove(&rel);
        self.pending_decl
            .store(inner.opt_in.pending_decl.len() as u64, Ordering::Relaxed);
        self.opt_in_total.fetch_add(1, Ordering::Relaxed);
        self.republish(&inner).await;
    }

    /// Take a rel out of scope (`replicate=false` / `TableRemoved`): drop its
    /// mapping + any pending decl, record the exclusion so republish keeps it
    /// out even when TOML-mapped, republish. In-flight
    /// rows already dispatched still drain; later transactions plan
    /// `route = None` discards.
    pub async fn exclude_table(&self, rel: &RelName) {
        let mut inner = self.inner.lock().await;
        inner.opt_in.mappings.remove(rel);
        inner.opt_in.derived.remove(rel);
        inner.opt_in.pending_decl.remove(rel);
        inner.opt_in.excluded.insert(rel.clone());
        self.pending_decl
            .store(inner.opt_in.pending_decl.len() as u64, Ordering::Relaxed);
        self.opt_out_total.fetch_add(1, Ordering::Relaxed);
        self.republish(&inner).await;
    }

    /// Park a `replicate=true` row whose rel isn't known yet; materialised when
    /// the matching `CREATE TABLE` arrives (see [`Self::take_pending_decl`]).
    /// No routing change, so no republish.
    pub async fn park_pending_decl(&self, rel: RelName, row: TableRow) {
        let mut inner = self.inner.lock().await;
        inner.opt_in.pending_decl.insert(rel, row);
        self.pending_decl
            .store(inner.opt_in.pending_decl.len() as u64, Ordering::Relaxed);
    }

    /// Remove and return a parked forward-declaration, if any.
    pub async fn take_pending_decl(&self, rel: &RelName) -> Option<TableRow> {
        let mut inner = self.inner.lock().await;
        let row = inner.opt_in.pending_decl.remove(rel);
        self.pending_decl
            .store(inner.opt_in.pending_decl.len() as u64, Ordering::Relaxed);
        row
    }

    pub async fn is_excluded(&self, rel: &RelName) -> bool {
        if self.inner.lock().await.opt_in.excluded.contains(rel) {
            return true;
        }
        self.tx.borrow().rules.settings(rel).replicate == Some(false)
    }

    /// Record an applicator-derived mapping (`auto_create` CREATE TABLE) so
    /// it survives republish, write the fenced routing map, republish. Runs
    /// inside the reorder barrier like the DDL that produced it.
    pub async fn register_derived_mapping(&self, rel: &RelName, mapping: TableMapping) {
        let mut inner = self.inner.lock().await;
        inner.opt_in.derived.insert(rel.clone(), mapping);
        self.republish(&inner).await;
    }

    /// Forget a runtime-derived mapping (source DROP TABLE under
    /// strategy=Drop) so a future `Added` re-derives columns. TOML-pinned
    /// mappings are untouched — republish rebuilds them from `base`, and
    /// `DdlApplicator::apply_added` re-creates their dest on a source
    /// re-create. An overlay `replicate=true` row re-parks as a
    /// forward-declaration so a re-create re-materialises the opt-in
    /// against the fresh descriptor.
    pub async fn forget_derived_mapping(&self, rel: &RelName) {
        let mut inner = self.inner.lock().await;
        inner.opt_in.derived.remove(rel);
        inner.opt_in.mappings.remove(rel);
        if let Some(row) = inner.overlay.tables.get(rel)
            && row.replicate == Some(true)
        {
            let row = row.clone();
            inner.opt_in.pending_decl.insert(rel.clone(), row);
            self.pending_decl
                .store(inner.opt_in.pending_decl.len() as u64, Ordering::Relaxed);
        }
        self.republish(&inner).await;
    }

    /// Fold an ALTER diff into the layer owning the rel's mapping so the
    /// auto-extension survives republish: opt-in / derived fold in place; a
    /// TOML-owned mapping folds copy-on-write into `derived` (which shadows
    /// `base` at resolve, so a SIGHUP TOML re-read can't revert the fold).
    /// Unmapped or excluded rels no-op without republish, mirroring
    /// `mutate_mapping_for_diff`'s early return.
    pub async fn apply_schema_diff(&self, new: &RelDescriptor, diff: &SchemaDiff) {
        let mut inner = self.inner.lock().await;
        let rel = &new.rel_name;
        if inner.opt_in.excluded.contains(rel) {
            return;
        }
        let rules = self.tx.borrow().column_rules.clone();
        if let Some(m) = inner.opt_in.mappings.get_mut(rel) {
            fold_diff_into_mapping(m, new, diff, &rules);
        } else if let Some(m) = inner.opt_in.derived.get_mut(rel) {
            fold_diff_into_mapping(m, new, diff, &rules);
        } else if let Some(base) = inner.base.tables.get(rel) {
            let mut m = base.clone();
            fold_diff_into_mapping(&mut m, new, diff, &rules);
            inner.opt_in.derived.insert(rel.clone(), m);
        } else {
            return;
        }
        self.republish(&inner).await;
    }

    /// CH target database for a namespace: per-namespace override (overlay then
    /// TOML) else the global `[ch] database`. Mirrors
    /// [`crate::emit::ch_ddl::DdlConfig::target_database_for`].
    fn target_db_for(inner: &MergeInputs, namespace: &str) -> String {
        inner
            .overlay
            .namespaces
            .get(namespace)
            .and_then(|r| r.target_database.clone())
            .or_else(|| {
                inner
                    .base
                    .namespaces
                    .get(namespace)
                    .and_then(|m| m.target_database.clone())
            })
            .unwrap_or_else(|| inner.base.database.clone())
    }

    /// Rebuild the resolved snapshot, write the fenced routing map, publish.
    async fn republish(&self, inner: &MergeInputs) {
        let prev = self.tx.borrow().clone();
        let (resolved, rejections) = Self::resolve(
            &inner.base,
            &inner.overlay,
            &self.cli,
            &inner.opt_in,
            &prev.column_rules,
        );
        self.rejections.store(rejections, Ordering::Relaxed);
        self.mapping
            .publish(Arc::new(resolved.tables.clone()))
            .await;
        tracing::info!(
            target: "walshadow::config",
            opt_ins = resolved.table_opt_ins.len(),
            tables = resolved.tables.len(),
            "republish",
        );
        // Err only when every receiver dropped (daemon tearing down); ignore
        let _ = self.tx.send(Arc::new(resolved));
    }

    /// Merge one snapshot: TOML base, then the PG overlay, then explicit CLI
    /// overrides on top. Returns the resolved config and the count of overlay
    /// values rejected as malformed (kept at the pre-overlay value, logged at
    /// WARN — Regime A: a bad row never crashes or freezes the pump).
    fn resolve(
        base: &EmitterConfig,
        overlay: &ConfigOverlay,
        cli: &CliOverrides,
        opt_in: &OptInState,
        prev_columns: &ColumnRules,
    ) -> (ResolvedConfig, u64) {
        let mut rejections = 0u64;
        // Runtime config overrides TOML at equal specificity
        let mut rules = TableRulesBuilder::new();
        for (rel, kind, rule) in &base.table_entries {
            rules.add(rel, *kind, rule.clone());
        }
        rules.next_layer();
        for (rel, row) in &overlay.tables {
            rules.add_row(rel, row);
        }
        let (rules, rule_rejections) = rules.finish();
        rejections += rule_rejections;
        let rules = Arc::new(rules);

        let mut column_rules = ColumnRulesBuilder::new();
        for e in &base.column_entries {
            column_rules.add(&e.rel, e.rel_kind, &e.attname, e.att_kind, e.rule.clone());
        }
        column_rules.next_layer();
        for ((rel, attname), row) in &overlay.columns {
            let kind = match MatchKind::parse(row.match_kind.as_deref().unwrap_or_default()) {
                Ok(k) => k,
                Err(e) => {
                    column_rules.bump_rejections();
                    tracing::warn!(target: "walshadow::config", qname = %rel, attname = %attname, error = %e, "config_column.match rejected");
                    continue;
                }
            };
            let Some(ty) = &row.target_type else {
                continue;
            };
            // Validate syntax now, validate wire compatibility with descriptor
            let accepted = if TypeAst::parse(ty, Allocator::stdlib()).is_ok() {
                Some(ty.as_str())
            } else {
                column_rules.bump_rejections();
                let prior = prev_columns.accepted_type(rel, attname);
                tracing::warn!(target: "walshadow::config", qname = %rel, attname = %attname, value = %ty, kept_prior = prior.is_some(), "config_column.target_type rejected: unparseable CH type");
                prior
            };
            if let Some(ty) = accepted {
                column_rules.record_accepted(rel, attname, ty);
                column_rules.add(
                    rel,
                    kind,
                    attname,
                    kind,
                    ColumnRule {
                        target_type: Some(ty.to_owned()),
                        ..ColumnRule::default()
                    },
                );
            }
        }
        let (column_rules, column_rejections) = column_rules.finish();
        rejections += column_rejections;
        let mut rc = ResolvedConfig {
            tables: base.tables.clone(),
            namespaces: base.namespaces.clone(),
            column_rules: Arc::new(column_rules),
            rules: rules.clone(),
            drop_table_strategy: base.drop_table_strategy.clone(),
            row_budget: base.row_budget,
            byte_budget: base.byte_budget,
            flush_timeout: base.flush_timeout,
            compression: base.compression,
            retry_max_attempts: base.retry.max_attempts,
            host: base.host.clone(),
            port: base.port,
            database: base.database.clone(),
            user: base.user.clone(),
            password: base.password.clone(),
            secure: base.secure,
            source: base.source.clone(),
            table_opt_ins: base.table_opt_ins.clone(),
            paused: base.paused,
        };

        // Runtime-derived layers (before the overlay target loop so a
        // `config_table` target row finds its mapping here). Both carry the
        // descriptor-derived projection a bare target row lacks; derived
        // first so an explicit opt-in wins over an auto-create.
        for (rel, mapping) in &opt_in.derived {
            rc.tables.insert(rel.clone(), mapping.clone());
        }
        for (rel, mapping) in &opt_in.mappings {
            rc.tables.insert(rel.clone(), mapping.clone());
        }

        // Layer 2: source-PG overlay.
        if let Some(g) = &overlay.global {
            if let Some(v) = &g.drop_table_strategy {
                if DropTableStrategy::parse(v).is_ok() {
                    rc.drop_table_strategy = v.clone();
                } else {
                    rejections += 1;
                    tracing::warn!(target: "walshadow::config", value = %v, "config_global.drop_table_strategy rejected");
                }
            }
            if let Some(v) = g.row_budget {
                match usize::try_from(v) {
                    Ok(u) if u > 0 => rc.row_budget = u,
                    _ => {
                        rejections += 1;
                        tracing::warn!(target: "walshadow::config", value = v, "config_global.row_budget rejected");
                    }
                }
            }
            if let Some(v) = g.byte_budget {
                match usize::try_from(v) {
                    Ok(u) if u > 0 => rc.byte_budget = u,
                    _ => {
                        rejections += 1;
                        tracing::warn!(target: "walshadow::config", value = v, "config_global.byte_budget rejected");
                    }
                }
            }
            if let Some(v) = g.flush_timeout_ms {
                match u64::try_from(v) {
                    Ok(ms) => rc.flush_timeout = Duration::from_millis(ms),
                    Err(_) => {
                        rejections += 1;
                        tracing::warn!(target: "walshadow::config", value = v, "config_global.flush_timeout_ms rejected");
                    }
                }
            }
            if let Some(v) = g.retry_max_attempts {
                match u32::try_from(v) {
                    Ok(n) => rc.retry_max_attempts = n,
                    Err(_) => {
                        rejections += 1;
                        tracing::warn!(target: "walshadow::config", value = v, "config_global.retry_max_attempts rejected");
                    }
                }
            }
            if let Some(v) = &g.compression {
                // Validate via build_codec so an unsupported-at-compile-time
                // codec (e.g. zstd with the feature off) is rejected here, never
                // surfaced as a fatal when the inserter reconnects.
                match CompressionChoice::parse(v).and_then(|c| c.build_codec().map(|_| c)) {
                    Ok(c) => rc.compression = c,
                    Err(_) => {
                        rejections += 1;
                        tracing::warn!(target: "walshadow::config", value = %v, "config_global.compression rejected");
                    }
                }
            }
        }

        for (ns, row) in &overlay.namespaces {
            let entry = rc.namespaces.entry(ns.clone()).or_default();
            if let Some(v) = &row.target_database {
                entry.target_database = Some(v.clone());
            }
            if let Some(v) = row.auto_create {
                entry.auto_create = v;
            }
            if let Some(v) = &row.drop_table_strategy {
                if DropTableStrategy::parse(v).is_ok() {
                    entry.drop_table_strategy = Some(v.clone());
                } else {
                    rejections += 1;
                    tracing::warn!(target: "walshadow::config", namespace = %ns, value = %v, "config_namespace.drop_table_strategy rejected");
                }
            }
        }

        for (rel, m) in rc.tables.iter_mut() {
            let settings = rules.settings(rel);
            if let Some(db) = settings.target_database {
                m.target.database = db;
            }
            if let Some(t) = settings.target_table {
                m.target.table = t;
            }
        }
        for (rel, row) in &overlay.tables {
            let names_target = row.target_database.is_some() || row.target_table.is_some();
            if names_target && !row.is_pattern() && !rc.tables.contains_key(rel) {
                tracing::warn!(
                    target: "walshadow::config",
                    qname = %rel,
                    "config_table target ignored: no mapping (set replicate=true to opt-in)",
                );
            }
        }

        // Apply exclusions after mappings
        for rel in &opt_in.excluded {
            rc.tables.remove(rel);
        }
        rc.tables
            .retain(|rel, _| rules.settings(rel).replicate != Some(false));

        // Layer 1: CLI (top). Survives SIGHUP + stale overlay rows.
        if let Some(v) = &cli.drop_table_strategy {
            rc.drop_table_strategy = v.clone();
        }
        if let Some(d) = cli.flush_timeout {
            rc.flush_timeout = d;
        }
        if cli.source_slot.is_some() {
            rc.source.slot = cli.source_slot.clone();
        }

        (rc, rejections)
    }

    /// Re-read the config (base `--ch-config` + conf.d, CLI-source base under
    /// it), re-merge with overlay + CLI, publish. Carries the CH connection +
    /// table opt-ins live; the source connection isn't in scope here. Parse /
    /// read errors surface to the caller and leave the last snapshot in effect.
    pub async fn reload(&self) -> Result<(), EmitterError> {
        let Some(path) = &self.toml_path else {
            return Ok(());
        };
        let merged = crate::ch_emitter::load_effective(path, self.cli_source_base.clone()).await?;
        let base = EmitterConfig::from_table(&merged)?;
        let mut inner = self.inner.lock().await;
        inner.base = base;
        self.republish(&inner).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_config::{GlobalRow, NamespaceRow, TableRow};
    use ahash::HashMapExt;

    fn base_with(drop_strategy: &str) -> EmitterConfig {
        EmitterConfig::from_toml_str(&format!(
            "[ch]\ndrop_table_strategy = \"{drop_strategy}\"\n"
        ))
        .unwrap()
    }

    fn dummy_handles() -> MappingHandle {
        crate::mapping::mapping_handle(HashMap::new())
    }

    #[test]
    fn cli_beats_overlay_beats_toml() {
        let base = base_with("retain");
        let overlay = ConfigOverlay {
            global: Some(GlobalRow {
                drop_table_strategy: Some("drop".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        // Overlay beats TOML.
        let (r, _) = ConfigResolver::resolve(
            &base,
            &overlay,
            &CliOverrides::default(),
            &OptInState::default(),
            &ColumnRules::default(),
        );
        assert_eq!(r.drop_table_strategy, "drop");
        // CLI beats overlay.
        let cli = CliOverrides {
            drop_table_strategy: Some("warn".into()),
            ..Default::default()
        };
        let (r, _) = ConfigResolver::resolve(
            &base,
            &overlay,
            &cli,
            &OptInState::default(),
            &ColumnRules::default(),
        );
        assert_eq!(r.drop_table_strategy, "warn");
    }

    #[test]
    fn toml_wins_when_overlay_and_cli_absent() {
        let base = base_with("drop");
        let (r, _) = ConfigResolver::resolve(
            &base,
            &ConfigOverlay::default(),
            &CliOverrides::default(),
            &OptInState::default(),
            &ColumnRules::default(),
        );
        assert_eq!(r.drop_table_strategy, "drop");
    }

    #[test]
    fn overlay_promotes_emitter_knobs() {
        let base = base_with("retain");
        // `none` is codec-feature-independent, unlike lz4/zstd; it also differs
        // from the Lz4 default so the override is observable.
        let overlay = ConfigOverlay {
            global: Some(GlobalRow {
                row_budget: Some(1000),
                flush_timeout_ms: Some(250),
                compression: Some("none".into()),
                retry_max_attempts: Some(9),
                ..Default::default()
            }),
            ..Default::default()
        };
        let (r, rej) = ConfigResolver::resolve(
            &base,
            &overlay,
            &CliOverrides::default(),
            &OptInState::default(),
            &ColumnRules::default(),
        );
        assert_eq!(rej, 0);
        assert_eq!(r.row_budget, 1000);
        assert_eq!(r.flush_timeout, Duration::from_millis(250));
        assert_eq!(r.compression, CompressionChoice::None);
        assert_eq!(r.retry_max_attempts, 9);
    }

    #[test]
    fn malformed_overlay_value_rejected_keeps_prior() {
        let base = base_with("retain");
        let overlay = ConfigOverlay {
            global: Some(GlobalRow {
                drop_table_strategy: Some("nonsense".into()),
                compression: Some("brotli".into()),
                row_budget: Some(-5),
                ..Default::default()
            }),
            ..Default::default()
        };
        let (r, rej) = ConfigResolver::resolve(
            &base,
            &overlay,
            &CliOverrides::default(),
            &OptInState::default(),
            &ColumnRules::default(),
        );
        assert_eq!(rej, 3);
        // Prior (TOML/base) values survive each rejection.
        assert_eq!(r.drop_table_strategy, "retain");
        assert_eq!(r.compression, base.compression);
        assert_eq!(r.row_budget, base.row_budget);
    }

    #[test]
    fn overlay_namespace_and_table_merge() {
        // config_table overrides the target of a TOML-mapped table (which
        // carries the column projection); the columns survive the override.
        let base = EmitterConfig::from_toml_str(
            "[ch]\ndrop_table_strategy = \"retain\"\n\
             [table.public.events]\ntarget_database = \"old\"\n\
             columns = [{ attnum = 1, target = \"id\", type = \"Int32\" }]\n",
        )
        .unwrap();
        let mut overlay = ConfigOverlay::default();
        overlay.namespaces.insert(
            "public".into(),
            NamespaceRow {
                auto_create: Some(true),
                target_database: Some("default".into()),
                drop_table_strategy: None,
            },
        );
        overlay.tables.insert(
            RelName::new("public", "events"),
            TableRow {
                target_database: Some("default".into()),
                ..Default::default()
            },
        );
        let (r, _) = ConfigResolver::resolve(
            &base,
            &overlay,
            &CliOverrides::default(),
            &OptInState::default(),
            &ColumnRules::default(),
        );
        let ns = r.namespaces.get("public").unwrap();
        assert!(ns.auto_create);
        assert_eq!(ns.target_database.as_deref(), Some("default"));
        let t = r.tables.get(&RelName::new("public", "events")).unwrap();
        assert_eq!(t.target, TableTarget::new("default", "events"));
        assert_eq!(
            t.columns.len(),
            1,
            "TOML columns preserved through override"
        );
    }

    #[test]
    fn pattern_opt_out_drops_a_toml_mapped_relation() {
        let base = EmitterConfig::from_toml_str(
            "[ch]\n\
             [table.app.tmp_scratch]\n\
             columns = [{ attnum = 1, target = \"id\", type = \"UInt64\" }]\n",
        )
        .unwrap();
        let mut overlay = ConfigOverlay::default();
        overlay.tables.insert(
            RelName::new("app", "tmp_*"),
            TableRow {
                match_kind: Some("glob".into()),
                replicate: Some(false),
                ..Default::default()
            },
        );
        let (r, _) = ConfigResolver::resolve(
            &base,
            &overlay,
            &CliOverrides::default(),
            &OptInState::default(),
            &ColumnRules::default(),
        );
        assert!(
            !r.tables.contains_key(&RelName::new("app", "tmp_scratch")),
            "an excluding pattern takes the mapping out of the routing map"
        );
    }

    #[test]
    fn overlay_pattern_scope_expands_over_present_relations() {
        let base = EmitterConfig::from_toml_str("[ch]\n").unwrap();
        let mut overlay = ConfigOverlay::default();
        overlay.tables.insert(
            RelName::new("app", "*_audit"),
            TableRow {
                match_kind: Some("glob".into()),
                replicate: Some(false),
                ..Default::default()
            },
        );
        overlay.tables.insert(
            RelName::new("app", "events_*"),
            TableRow {
                match_kind: Some("glob".into()),
                replicate: Some(true),
                initial_load: Some("copy".into()),
                ..Default::default()
            },
        );
        let (r, _) = ConfigResolver::resolve(
            &base,
            &overlay,
            &CliOverrides::default(),
            &OptInState::default(),
            &ColumnRules::default(),
        );
        let present = [
            RelName::new("app", "events_1"),
            RelName::new("app", "events_audit"),
            RelName::new("app", "orders"),
        ];
        let scoped = r.rules.pattern_scoped(|| present.to_vec(), |_| false);
        assert_eq!(scoped.len(), 2, "opt-in and opt-out both dispatch");
        let opted: Vec<_> = scoped
            .iter()
            .filter(|(_, row)| row.replicate == Some(true))
            .map(|(rel, _)| rel.name.to_string())
            .collect();
        assert_eq!(opted, ["events_1"]);
        assert_eq!(
            r.rules.settings(&present[1]).replicate,
            Some(false),
            "an excluding pattern is a guardrail: it beats a matching opt-in"
        );
    }

    #[test]
    fn overlay_pattern_retargets_a_mapped_relation() {
        let base = EmitterConfig::from_toml_str(
            "[ch]\n\
             [table.app.events_1]\n\
             columns = [{ attnum = 1, target = \"id\", type = \"UInt64\" }]\n",
        )
        .unwrap();
        let mut overlay = ConfigOverlay::default();
        overlay.tables.insert(
            RelName::new("app", "events_*"),
            TableRow {
                match_kind: Some("glob".into()),
                target_database: Some("warehouse".into()),
                ..Default::default()
            },
        );
        let (r, _) = ConfigResolver::resolve(
            &base,
            &overlay,
            &CliOverrides::default(),
            &OptInState::default(),
            &ColumnRules::default(),
        );
        let t = r.tables.get(&RelName::new("app", "events_1")).unwrap();
        assert_eq!(t.target.database, "warehouse");
        assert_eq!(t.columns.len(), 1, "TOML projection preserved");
    }

    #[test]
    fn overlay_bad_pattern_rejected_and_counted() {
        let base = EmitterConfig::from_toml_str("[ch]\n").unwrap();
        let mut overlay = ConfigOverlay::default();
        overlay.tables.insert(
            RelName::new("app", "ev(nt"),
            TableRow {
                match_kind: Some("regex".into()),
                replicate: Some(true),
                ..Default::default()
            },
        );
        overlay.tables.insert(
            RelName::new("app", "orders"),
            TableRow {
                match_kind: Some("like".into()),
                ..Default::default()
            },
        );
        let (r, rejections) = ConfigResolver::resolve(
            &base,
            &overlay,
            &CliOverrides::default(),
            &OptInState::default(),
            &ColumnRules::default(),
        );
        assert_eq!(rejections, 2, "unparseable regex + unknown match kind");
        assert!(!r.rules.has_patterns());
    }

    #[test]
    fn overlay_row_overrides_toml_entry() {
        let base = EmitterConfig::from_toml_str(
            "[ch]\n\
             [table.app.\"events_*\"]\n\
             match = \"glob\"\n\
             replicate = true\n\
             target_database = \"toml_db\"\n",
        )
        .unwrap();
        let mut overlay = ConfigOverlay::default();
        overlay.tables.insert(
            RelName::new("app", "events_*"),
            TableRow {
                match_kind: Some("glob".into()),
                target_database: Some("sql_db".into()),
                ..Default::default()
            },
        );
        let (r, _) = ConfigResolver::resolve(
            &base,
            &overlay,
            &CliOverrides::default(),
            &OptInState::default(),
            &ColumnRules::default(),
        );
        let rel = RelName::new("app", "events_2026");
        let s = r.rules.settings(&rel);
        assert_eq!(s.target_database.as_deref(), Some("sql_db"));
        assert_eq!(s.replicate, Some(true), "TOML scope still applies");
        let ddl =
            crate::emit::ch_ddl::DdlConfig::from_resolved(&r, "db".into(), false, false, None);
        assert_eq!(ddl.declared_scope(&rel), Some(true));
    }

    fn auto_create_set(base: &EmitterConfig, overlay: &ConfigOverlay) -> ahash::HashSet<String> {
        use crate::emit::ch_ddl::DdlConfig;
        let (r, _) = ConfigResolver::resolve(
            base,
            overlay,
            &CliOverrides::default(),
            &OptInState::default(),
            &ColumnRules::default(),
        );
        DdlConfig::from_resolved(&r, "db".into(), false, false, None).auto_create_namespaces
    }

    #[test]
    fn ddl_config_auto_create_from_toml() {
        let base = EmitterConfig::from_toml_str(
            "[ch]\n[namespace.s1]\nauto_create = true\n[namespace.s2]\nauto_create = false\n",
        )
        .unwrap();
        let ns = auto_create_set(&base, &ConfigOverlay::default());
        assert!(ns.contains("s1"), "TOML auto_create=true enables");
        assert!(!ns.contains("s2"), "TOML auto_create=false stays off");
    }

    #[test]
    fn ddl_config_auto_create_from_overlay() {
        // No TOML namespace; the overlay alone turns it on.
        let base = base_with("retain");
        let mut overlay = ConfigOverlay::default();
        overlay.namespaces.insert(
            "s1".into(),
            NamespaceRow {
                auto_create: Some(true),
                ..Default::default()
            },
        );
        let ns = auto_create_set(&base, &overlay);
        assert!(ns.contains("s1"), "overlay auto_create=true enables");
    }

    #[test]
    fn overlay_auto_create_overrides_toml() {
        // TOML false, overlay true → enabled.
        let base =
            EmitterConfig::from_toml_str("[ch]\n[namespace.s1]\nauto_create = false\n").unwrap();
        let mut overlay = ConfigOverlay::default();
        overlay.namespaces.insert(
            "s1".into(),
            NamespaceRow {
                auto_create: Some(true),
                ..Default::default()
            },
        );
        assert!(
            auto_create_set(&base, &overlay).contains("s1"),
            "overlay true beats TOML false"
        );

        // TOML true, overlay false → disabled.
        let base =
            EmitterConfig::from_toml_str("[ch]\n[namespace.s1]\nauto_create = true\n").unwrap();
        let mut overlay = ConfigOverlay::default();
        overlay.namespaces.insert(
            "s1".into(),
            NamespaceRow {
                auto_create: Some(false),
                ..Default::default()
            },
        );
        assert!(
            !auto_create_set(&base, &overlay).contains("s1"),
            "overlay false beats TOML true"
        );
    }

    #[test]
    fn ddl_config_no_auto_create_when_unset() {
        let base = base_with("retain");
        assert!(
            auto_create_set(&base, &ConfigOverlay::default()).is_empty(),
            "no source sets auto_create → empty set"
        );
    }

    fn rel_desc(namespace: &str, name: &str) -> RelDescriptor {
        use crate::schema::{RelAttr, ReplIdent};
        use walrus::pg::walparser::RelFileNode;
        RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 30000,
            },
            oid: 30000,
            toast_oid: 0,
            namespace_oid: 2200,
            rel_name: RelName::new(namespace, name),
            kind: 'r',
            persistence: 'p',
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: vec![RelAttr {
                attnum: 1,
                name: "id".into(),
                type_oid: 23, // int4, bridges cleanly
                typmod: -1,
                not_null: true,
                dropped: false,
                type_name: "int4".into(),
                type_byval: true,
                type_len: 4,
                type_align: 'i',
                type_storage: 'p',
                missing_text: None,
            }],
        }
    }

    #[test]
    fn opt_in_mapping_overlays_and_exclusion_removes() {
        // An opt-in mapping lands in resolved.tables with no TOML entry; an
        // excluded qname is dropped even when TOML-mapped.
        let base = EmitterConfig::from_toml_str(
            "[ch]\n\
             [table.public.keep]\ntarget_database = \"old\"\n\
             columns = [{ attnum = 1, target = \"id\", type = \"Int32\" }]\n",
        )
        .unwrap();
        let mut opt_in = OptInState::default();
        opt_in.mappings.insert(
            RelName::new("public", "events"),
            TableMapping {
                target: TableTarget::new("default", "events"),
                columns: Vec::new(),
            },
        );
        opt_in.excluded.insert(RelName::new("public", "keep"));
        let (r, _) = ConfigResolver::resolve(
            &base,
            &ConfigOverlay::default(),
            &CliOverrides::default(),
            &opt_in,
            &ColumnRules::default(),
        );
        assert!(
            r.tables.contains_key(&RelName::new("public", "events")),
            "opt-in included"
        );
        assert!(
            !r.tables.contains_key(&RelName::new("public", "keep")),
            "excluded rel dropped even when TOML-mapped"
        );
    }

    #[tokio::test]
    async fn materialize_opt_in_derives_maps() {
        let base = base_with("retain");
        let mapping = dummy_handles();
        let (resolver, mut rx) = ConfigResolver::new(
            &base,
            CliOverrides::default(),
            None,
            toml::Table::new(),
            mapping.clone(),
        );
        resolver
            .materialize_opt_in(&rel_desc("public", "events"), None, None)
            .await;
        assert!(rx.changed().await.is_ok());
        let snap = rx.borrow_and_update();
        let rel = RelName::new("public", "events");
        let t = snap.tables.get(&rel).expect("mapping present");
        assert_eq!(t.target.table, "events", "target derived from descriptor");
        assert!(!t.columns.is_empty(), "columns derived from descriptor");
        // Fenced routing map written for the decode pool.
        assert!(mapping.with(|m| m.contains_key(&rel)).await);
        assert_eq!(resolver.opt_in_total(), 1);
    }

    #[tokio::test]
    async fn exclude_table_removes_mapping() {
        let base = EmitterConfig::from_toml_str(
            "[ch]\n[table.public.events]\n\
             columns = [{ attnum = 1, target = \"id\", type = \"Int32\" }]\n",
        )
        .unwrap();
        let mapping = dummy_handles();
        let (resolver, mut rx) = ConfigResolver::new(
            &base,
            CliOverrides::default(),
            None,
            toml::Table::new(),
            mapping.clone(),
        );
        let rel = RelName::new("public", "events");
        assert!(rx.borrow().tables.contains_key(&rel));
        resolver.exclude_table(&rel).await;
        assert!(rx.changed().await.is_ok());
        assert!(
            !rx.borrow_and_update().tables.contains_key(&rel),
            "opt-out drops the mapping"
        );
        assert!(!mapping.with(|m| m.contains_key(&rel)).await);
        assert_eq!(resolver.opt_out_total(), 1);
    }

    #[tokio::test]
    async fn derived_mapping_survives_republish() {
        let base = base_with("retain");
        let mapping = dummy_handles();
        let (resolver, mut rx) = ConfigResolver::new(
            &base,
            CliOverrides::default(),
            None,
            toml::Table::new(),
            mapping.clone(),
        );
        let rel = RelName::new("public", "auto");
        resolver
            .register_derived_mapping(
                &rel,
                TableMapping {
                    target: TableTarget::new("default", "auto"),
                    columns: Vec::new(),
                },
            )
            .await;
        assert!(rx.changed().await.is_ok());
        assert!(rx.borrow_and_update().tables.contains_key(&rel));
        assert!(mapping.with(|m| m.contains_key(&rel)).await);
        // An unrelated overlay apply full-swaps the handle; the derived
        // mapping must survive (the [config.md] "Known limitation" clobber)
        resolver
            .apply_config_event(ConfigEvent::GlobalCleared)
            .await;
        assert!(rx.changed().await.is_ok());
        assert!(rx.borrow_and_update().tables.contains_key(&rel));
        assert!(mapping.with(|m| m.contains_key(&rel)).await);
        // Source DROP TABLE under strategy=Drop forgets it everywhere
        resolver.forget_derived_mapping(&rel).await;
        assert!(!mapping.with(|m| m.contains_key(&rel)).await);
        assert!(rx.changed().await.is_ok());
        assert!(!rx.borrow_and_update().tables.contains_key(&rel));
    }

    #[tokio::test]
    async fn forget_reparks_opt_in_row_as_pending_decl() {
        let base = base_with("drop");
        let mapping = dummy_handles();
        let (resolver, _rx) = ConfigResolver::new(
            &base,
            CliOverrides::default(),
            None,
            toml::Table::new(),
            mapping.clone(),
        );
        let rel = RelName::new("public", "events");
        resolver
            .apply_config_event(ConfigEvent::TableUpserted {
                rel: rel.clone(),
                row: TableRow {
                    replicate: Some(true),
                    ..Default::default()
                },
            })
            .await;
        resolver
            .materialize_opt_in(&rel_desc("public", "events"), None, None)
            .await;
        assert!(mapping.with(|m| m.contains_key(&rel)).await);
        // Source DROP under strategy=Drop: mapping forgotten, opt-in row
        // re-parked so the next CREATE re-materialises it
        resolver.forget_derived_mapping(&rel).await;
        assert!(!mapping.with(|m| m.contains_key(&rel)).await);
        assert_eq!(resolver.pending_decl_count(), 1);
        let row = resolver.take_pending_decl(&rel).await.expect("re-parked");
        assert_eq!(row.replicate, Some(true));
    }

    #[tokio::test]
    async fn schema_diff_fold_survives_republish() {
        use crate::schema::RelAttr;
        // TOML-owned mapping: the fold lands copy-on-write in the derived
        // layer, so neither a config apply nor a SIGHUP re-merge reverts it
        let base = EmitterConfig::from_toml_str(
            "[ch]\n[table.public.events]\n\
             columns = [{ attnum = 1, target = \"id\", type = \"Int32\" }]\n",
        )
        .unwrap();
        let mapping = dummy_handles();
        let (resolver, _rx) = ConfigResolver::new(
            &base,
            CliOverrides::default(),
            None,
            toml::Table::new(),
            mapping.clone(),
        );
        let mut desc = rel_desc("public", "events");
        desc.attributes.push(RelAttr {
            attnum: 2,
            name: "note".into(),
            type_oid: 25, // text
            typmod: -1,
            not_null: false,
            dropped: false,
            type_name: "text".into(),
            type_byval: false,
            type_len: -1,
            type_align: 'i',
            type_storage: 'x',
            missing_text: None,
        });
        let diff = SchemaDiff {
            added_columns: vec![desc.attributes[1].clone()],
            dropped_columns: vec![],
            renamed_columns: vec![],
            type_changes: vec![],
        };
        resolver.apply_schema_diff(&desc, &diff).await;
        let has_note = |m: &HashMap<RelName, TableMapping>| {
            m.get(&RelName::new("public", "events"))
                .is_some_and(|t| t.columns.iter().any(|c| c.src_attnum == 2))
        };
        assert!(
            mapping.with(|m| has_note(m)).await,
            "fold reaches the handle"
        );
        resolver
            .apply_config_event(ConfigEvent::GlobalCleared)
            .await;
        assert!(
            mapping.with(|m| has_note(m)).await,
            "fold survives the republish full-swap"
        );
    }

    #[test]
    fn column_override_validated_at_merge() {
        use crate::runtime_config::ColumnRow;
        let base = base_with("retain");
        let mut overlay = ConfigOverlay::default();
        overlay.columns.insert(
            (RelName::new("public", "t"), "amount".into()),
            ColumnRow {
                target_type: Some("Int128".into()),
                ..ColumnRow::default()
            },
        );
        overlay.columns.insert(
            (RelName::new("public", "t"), "bad".into()),
            ColumnRow {
                target_type: Some("NotAType(".into()),
                ..ColumnRow::default()
            },
        );
        let (r, rej) = ConfigResolver::resolve(
            &base,
            &overlay,
            &CliOverrides::default(),
            &OptInState::default(),
            &ColumnRules::default(),
        );
        assert_eq!(rej, 1, "unparseable type rejected");
        let rel = RelName::new("public", "t");
        assert_eq!(
            r.column_rules.settings(&rel, "amount").target_type,
            Some("Int128".into())
        );
        assert!(r.column_rules.settings(&rel, "bad").target_type.is_none());
    }

    #[test]
    fn column_rules_layer_toml_under_a_pattern_overlay_row() {
        use crate::runtime_config::ColumnRow;
        let base = EmitterConfig::from_toml_str(
            "[ch]\n\
             [table.app.events]\n\
             replicate = true\n\
             columns = [{ name = \"amount\", target = \"amt\", type = \"Decimal(38, 2)\" }]\n",
        )
        .unwrap();
        let mut overlay = ConfigOverlay::default();
        overlay.columns.insert(
            (RelName::new("app", "*"), "amount".into()),
            ColumnRow {
                target_type: Some("Int128".into()),
                match_kind: Some("glob".into()),
            },
        );
        let (r, rej) = ConfigResolver::resolve(
            &base,
            &overlay,
            &CliOverrides::default(),
            &OptInState::default(),
            &ColumnRules::default(),
        );
        assert_eq!(rej, 0);
        let s = r
            .column_rules
            .settings(&RelName::new("app", "events"), "amount");
        assert_eq!(
            s.target_name.as_deref(),
            Some("amt"),
            "TOML names the CH column: the overlay row states no name"
        );
        assert_eq!(
            s.target_type.as_deref(),
            Some("Decimal(38, 2)"),
            "a literal entry outranks a pattern however late its layer"
        );
        assert_eq!(
            r.column_rules
                .settings(&RelName::new("app", "orders"), "amount")
                .target_type
                .as_deref(),
            Some("Int128")
        );
        assert!(
            r.column_rules
                .settings(&RelName::new("other", "events"), "amount")
                .target_type
                .is_none()
        );
        overlay.columns.insert(
            (RelName::new("app", "events"), "amount".into()),
            ColumnRow {
                target_type: Some("Int128".into()),
                match_kind: None,
            },
        );
        let (r, _) = ConfigResolver::resolve(
            &base,
            &overlay,
            &CliOverrides::default(),
            &OptInState::default(),
            &ColumnRules::default(),
        );
        let s = r
            .column_rules
            .settings(&RelName::new("app", "events"), "amount");
        assert_eq!(s.target_type.as_deref(), Some("Int128"));
        assert_eq!(s.target_name.as_deref(), Some("amt"), "name still TOML's");
    }

    #[test]
    fn column_override_invalid_update_keeps_last_accepted() {
        use crate::runtime_config::ColumnRow;
        let base = base_with("retain");
        let key = (RelName::new("public", "t"), "amount".to_owned());
        let mut overlay = ConfigOverlay::default();
        overlay.columns.insert(
            key.clone(),
            ColumnRow {
                target_type: Some("Decimal(38, 2)".into()),
                ..ColumnRow::default()
            },
        );
        let (first, rej) = ConfigResolver::resolve(
            &base,
            &overlay,
            &CliOverrides::default(),
            &OptInState::default(),
            &ColumnRules::default(),
        );
        assert_eq!(rej, 0);
        // Malformed update replaces the overlay row wholesale; merge keeps
        // the accepted value off the previous snapshot
        overlay.columns.insert(
            key,
            ColumnRow {
                target_type: Some("NotAType(".into()),
                ..ColumnRow::default()
            },
        );
        let (second, rej) = ConfigResolver::resolve(
            &base,
            &overlay,
            &CliOverrides::default(),
            &OptInState::default(),
            &first.column_rules,
        );
        assert_eq!(rej, 1);
        let amount = |r: &ResolvedConfig| {
            r.column_rules
                .settings(&RelName::new("public", "t"), "amount")
                .target_type
        };
        assert_eq!(amount(&second).as_deref(), Some("Decimal(38, 2)"));
        // Retention carries forward while the bad row stays in the overlay
        let (third, rej) = ConfigResolver::resolve(
            &base,
            &overlay,
            &CliOverrides::default(),
            &OptInState::default(),
            &second.column_rules,
        );
        assert_eq!(rej, 1);
        assert_eq!(amount(&third).as_deref(), Some("Decimal(38, 2)"));
    }

    #[tokio::test]
    async fn column_override_survives_malformed_update_until_removed() {
        use crate::runtime_config::ColumnRow;
        let base = base_with("retain");
        let mapping = dummy_handles();
        let (resolver, mut rx) = ConfigResolver::new(
            &base,
            CliOverrides::default(),
            None,
            toml::Table::new(),
            mapping,
        );
        let upsert = |ty: &str| ConfigEvent::ColumnUpserted {
            rel: RelName::new("public", "t"),
            attname: "amount".into(),
            row: ColumnRow {
                target_type: Some(ty.into()),
                ..ColumnRow::default()
            },
        };
        resolver.apply_config_event(upsert("Decimal(38, 2)")).await;
        assert!(rx.changed().await.is_ok());
        assert_eq!(
            rx.borrow_and_update()
                .column_rules
                .settings(&RelName::new("public", "t"), "amount")
                .target_type
                .as_deref(),
            Some("Decimal(38, 2)")
        );
        resolver.apply_config_event(upsert("NotAType(")).await;
        assert!(rx.changed().await.is_ok());
        assert_eq!(
            rx.borrow_and_update()
                .column_rules
                .settings(&RelName::new("public", "t"), "amount")
                .target_type
                .as_deref(),
            Some("Decimal(38, 2)"),
            "malformed update keeps last accepted override"
        );
        assert_eq!(resolver.rejections(), 1);
        // Explicit DELETE clears; retention applies to bad updates only
        resolver
            .apply_config_event(ConfigEvent::ColumnRemoved {
                rel: RelName::new("public", "t"),
                attname: "amount".into(),
            })
            .await;
        assert!(rx.changed().await.is_ok());
        assert!(rx.borrow_and_update().column_rules.is_empty());
        assert_eq!(resolver.rejections(), 0, "gauge clears with the bad row");
    }

    #[tokio::test]
    async fn pending_decl_parks_and_takes() {
        let base = base_with("retain");
        let mapping = dummy_handles();
        let (resolver, _rx) = ConfigResolver::new(
            &base,
            CliOverrides::default(),
            None,
            toml::Table::new(),
            mapping,
        );
        let rel = RelName::new("app", "later");
        resolver
            .park_pending_decl(rel.clone(), TableRow::default())
            .await;
        assert_eq!(resolver.pending_decl_count(), 1);
        assert!(resolver.take_pending_decl(&rel).await.is_some());
        assert_eq!(resolver.pending_decl_count(), 0);
        assert!(resolver.take_pending_decl(&rel).await.is_none());
    }

    #[tokio::test]
    async fn seed_and_apply_republish() {
        let base = base_with("retain");
        let mapping = dummy_handles();
        let (resolver, mut rx) = ConfigResolver::new(
            &base,
            CliOverrides::default(),
            None,
            toml::Table::new(),
            mapping,
        );
        assert_eq!(rx.borrow().drop_table_strategy, "retain");

        let overlay = ConfigOverlay {
            global: Some(GlobalRow {
                drop_table_strategy: Some("drop".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        resolver.seed_overlay(overlay).await;
        assert!(rx.changed().await.is_ok());
        assert_eq!(rx.borrow_and_update().drop_table_strategy, "drop");

        resolver
            .apply_config_event(ConfigEvent::GlobalCleared)
            .await;
        assert!(rx.changed().await.is_ok());
        assert_eq!(rx.borrow_and_update().drop_table_strategy, "retain");
    }

    #[tokio::test]
    async fn reload_without_path_is_noop() {
        let base = base_with("retain");
        let mapping = dummy_handles();
        let (resolver, rx) = ConfigResolver::new(
            &base,
            CliOverrides::default(),
            None,
            toml::Table::new(),
            mapping,
        );
        resolver.reload().await.unwrap();
        assert_eq!(rx.borrow().drop_table_strategy, "retain");
    }

    #[test]
    fn source_conn_parses_string_and_integer_port() {
        let root: toml::Table = toml::from_str(
            "[source]\nhost = \"db.internal\"\nport = 5433\nuser = \"repl\"\n\
             password = \"pw\"\ndbname = \"app\"\nsslmode = \"require\"\n",
        )
        .unwrap();
        let conn = SourceConn::from_table(&root).unwrap();
        assert_eq!(conn.host, "db.internal");
        assert_eq!(conn.port, 5433);
        assert_eq!(conn.user, "repl");
        assert_eq!(conn.password.as_deref(), Some("pw"));
        assert_eq!(conn.dbname, "app");
        assert_eq!(conn.sslmode, SslMode::Require);
        assert_eq!(conn.endpoint(), "db.internal:5433/app");
        // No password in Debug, which ResolvedConfig inherits
        assert!(!format!("{conn:?}").contains("pw"));

        // The CLI base layer writes a bare integer, files may quote it
        let quoted: toml::Table = toml::from_str("[source]\nport = \"5434\"\n").unwrap();
        assert_eq!(SourceConn::from_table(&quoted).unwrap().port, 5434);
    }

    #[test]
    fn source_conn_absent_section_keeps_defaults() {
        let conn = SourceConn::from_table(&toml::Table::new()).unwrap();
        assert_eq!(conn, SourceConn::default());
    }

    /// `ctl apply` validates through `EmitterConfig::from_table`, so a typo
    /// here is rejected and rolled back instead of reloading the pump onto an
    /// endpoint it cannot dial
    #[test]
    fn source_conn_rejects_bad_sslmode_and_port() {
        let bad_ssl: toml::Table = toml::from_str("[source]\nsslmode = \"maybe\"\n").unwrap();
        assert!(SourceConn::from_table(&bad_ssl).is_err());
        assert!(EmitterConfig::from_table(&bad_ssl).is_err());
        let bad_port: toml::Table = toml::from_str("[source]\nport = 99999\n").unwrap();
        assert!(SourceConn::from_table(&bad_port).is_err());
    }

    /// The pump diffs `ResolvedConfig.source` to decide a feed swap, so the
    /// endpoint has to survive the TOML → EmitterConfig → resolve path
    #[test]
    fn resolve_carries_source_endpoint() {
        let base = EmitterConfig::from_toml_str(
            "[ch]\nhost = \"ch\"\n[source]\nhost = \"pg-b\"\nport = 5433\ndbname = \"app\"\n",
        )
        .unwrap();
        let (r, _) = ConfigResolver::resolve(
            &base,
            &ConfigOverlay::default(),
            &CliOverrides::default(),
            &OptInState::default(),
            &ColumnRules::default(),
        );
        assert_eq!(r.source.host, "pg-b");
        assert_eq!(r.source.port, 5433);
        assert_ne!(r.source, SourceConn::default());
    }

    /// A promotion target can name its slot its own way, so the name reloads
    /// with the endpoint; `--slot` pins it for the process either way
    #[test]
    fn resolve_carries_source_slot() {
        let base = EmitterConfig::from_toml_str(
            "[ch]\nhost = \"ch\"\n[source]\nhost = \"pg-b\"\nslot = \"target_phys\"\n",
        )
        .unwrap();
        let (r, _) = ConfigResolver::resolve(
            &base,
            &ConfigOverlay::default(),
            &CliOverrides::default(),
            &OptInState::default(),
            &ColumnRules::default(),
        );
        assert_eq!(r.source.slot.as_deref(), Some("target_phys"));

        let cli = CliOverrides {
            source_slot: Some("pinned".into()),
            ..CliOverrides::default()
        };
        let (r, _) = ConfigResolver::resolve(
            &base,
            &ConfigOverlay::default(),
            &cli,
            &OptInState::default(),
            &ColumnRules::default(),
        );
        assert_eq!(r.source.slot.as_deref(), Some("pinned"));
    }

    #[tokio::test]
    async fn reload_republishes_moved_source_endpoint() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ch-config.toml");
        tokio::fs::write(&path, "[ch]\nhost = \"ch\"\n[source]\nhost = \"pg-a\"\n")
            .await
            .unwrap();
        let base = EmitterConfig::from_toml_str("[ch]\nhost = \"ch\"\n[source]\nhost = \"pg-a\"\n")
            .unwrap();
        let (resolver, mut rx) = ConfigResolver::new(
            &base,
            CliOverrides::default(),
            Some(path.clone()),
            toml::Table::new(),
            dummy_handles(),
        );
        assert_eq!(rx.borrow_and_update().source.host, "pg-a");

        tokio::fs::write(&path, "[ch]\nhost = \"ch\"\n[source]\nhost = \"pg-b\"\n")
            .await
            .unwrap();
        resolver.reload().await.unwrap();
        assert_eq!(rx.borrow_and_update().source.host, "pg-b");
    }

    /// Backfill caches `overlay_dest` and rebuilds on `!dest_conn_eq`, so the
    /// overlay has to settle in one pass — a field written but not compared
    /// deep-copies the mapping tables on every session the pass opens.
    #[test]
    fn overlay_dest_moves_connection_and_keeps_tuning() {
        let boot =
            EmitterConfig::from_toml_str("[ch]\nhost = \"ch-a\"\nport = 9000\nrow_budget = 4321\n")
                .unwrap();
        let live = ResolvedConfig {
            host: "ch-b".into(),
            port: 9440,
            database: "db".into(),
            user: "u".into(),
            password: "p".into(),
            secure: true,
            ..ResolvedConfig::default()
        };

        let out = live.overlay_dest(&boot);
        assert_eq!(out.host, "ch-b");
        assert_eq!(out.port, 9440);
        assert!(out.secure);
        assert_eq!(out.row_budget, boot.row_budget);
        assert!(!live.dest_conn_eq(&boot));
        assert!(live.dest_conn_eq(&out));
    }

    /// The reverse drift: a moved field the comparison skips never reaches the
    /// sessions, which keep dialling the old endpoint
    #[test]
    fn dest_conn_eq_notices_every_overlaid_field() {
        let live = ResolvedConfig::default();
        let base = live.overlay_dest(&EmitterConfig::default());
        let moves: [fn(&mut EmitterConfig); 6] = [
            |c| c.host.push('x'),
            |c| c.port += 1,
            |c| c.database.push('x'),
            |c| c.user.push('x'),
            |c| c.password.push('x'),
            |c| c.secure = !c.secure,
        ];
        for apply in moves {
            let mut moved = base.clone();
            apply(&mut moved);
            assert!(!live.dest_conn_eq(&moved));
        }
    }
}
