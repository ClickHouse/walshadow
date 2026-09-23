//! In-process control plane over a Unix socket
//!
//! TOML bodies preserve config types and let one request update several
//! sections atomically. Mutations only touch one `ch-config.d` fragment,
//! `50-api.toml` unless `--control-fragment` names another, keeping
//! operator-owned config read-only. PeerDB shim consumes this protocol

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;
use tokio_postgres::Client;
use toml::{Table, Value};

use walrus::pg::backup::format_pg_lsn;

use crate::config::SourceConn;
use crate::introspect;
use crate::metrics::MetricsRegistry;
use crate::schema::RelName;
use crate::source_feed::open_sql_client;

/// Holds the running session's resolver so the control socket + SIGHUP can
/// trigger a live `reload()`. The daemon streams one session; there is no
/// start/stop/restart lifecycle — pause is a config flag applied by reload.
#[derive(Default)]
pub struct Reloader {
    /// One per followed database: a reload republishes each database's own
    /// scope of the same document
    resolvers: Mutex<Vec<Arc<crate::config::ConfigResolver>>>,
}

impl Reloader {
    pub async fn set_resolvers(&self, r: Vec<Arc<crate::config::ConfigResolver>>) {
        *self.resolvers.lock().await = r.clone();
        // Control socket serves before the session wires a resolver;
        // apply/reload in that window persist fragments but republish
        // nothing (reload() below no-ops while empty). Sweep once at wiring
        // so file state and published config converge
        for r in &r {
            if let Err(e) = r.reload().await {
                tracing::warn!(
                    target: "walshadow::control",
                    error = %e,
                    "config sweep at resolver wiring failed",
                );
            }
        }
    }

    /// Live reconfigure: re-read the merged config + republish. No restart.
    /// Return databases the config now names that this process does not
    /// follow: shadow registers its bridge workers at startup, so they need a
    /// restart rather than a reload
    pub async fn reload(&self) -> anyhow::Result<Vec<String>> {
        let resolvers = self.resolvers.lock().await.clone();
        for r in &resolvers {
            r.reload()
                .await
                .map_err(|e| anyhow::anyhow!("reload: {e}"))?;
        }
        let Some(first) = resolvers.first() else {
            return Ok(Vec::new());
        };
        let mut unfollowed = first.configured_databases().await;
        unfollowed.retain(|db| !resolvers.iter().any(|r| r.dbname() == db));
        Ok(unfollowed)
    }
}

// ---------------------------------------------------------------------------
// Shared context handed to the socket handlers
// ---------------------------------------------------------------------------

/// The managed TOML config path + read handles. No config struct — the file is
/// the source of truth.
#[derive(Clone)]
pub struct SharedCtx {
    pub ch_config: PathBuf,
    /// Fragment control mutations write, see [`fragment_path`]
    pub fragment: PathBuf,
    /// CLI-arg `[source]` / `[ch]` defaults; the config file overrides them,
    /// matching the daemon's connection resolution
    /// (see `ch_emitter::load_effective`).
    pub cli_base: Table,
    pub metrics: MetricsRegistry,
    pub reloader: Arc<Reloader>,
    /// Prevents concurrent fragment updates from overwriting each other
    pub frag_lock: Arc<Mutex<()>>,
}

// ---------------------------------------------------------------------------
// TOML request protocol
// ---------------------------------------------------------------------------

/// Keeps CLI and PeerDB shim request framing consistent
pub fn encode_request(verb: &str, config: Table) -> Result<String> {
    let body = toml::to_string(&config).context("serialize request config")?;
    Ok(format!("{verb}\n{body}"))
}

pub struct Request<'a> {
    pub verb: &'a str,
    pub config: Table,
}

impl<'a> Request<'a> {
    /// Preserves TOML types and quoted values across control socket
    pub fn parse(buf: &'a [u8]) -> Result<Request<'a>> {
        let text = std::str::from_utf8(buf).context("request not utf-8")?;
        let (head, body) = text.split_once('\n').unwrap_or((text, ""));
        let verb = head.split_whitespace().next().context("empty request")?;
        let config = if body.trim().is_empty() {
            Table::new()
        } else {
            body.parse().context("parse request config toml")?
        };
        Ok(Request { verb, config })
    }
}

pub fn ok() -> String {
    "OK\n".into()
}
/// `OK` plus what a reload could not act on, which `ctl` prints as-is
fn ok_note(unfollowed: &[String]) -> String {
    if unfollowed.is_empty() {
        return ok();
    }
    format!(
        "OK config names {} this process does not follow; restart to replicate\n",
        unfollowed.join(", "),
    )
}

pub fn ok_with(body: &str) -> String {
    if body.is_empty() {
        ok()
    } else if body.ends_with('\n') {
        format!("OK\n{body}")
    } else {
        format!("OK\n{body}\n")
    }
}
pub fn err(msg: impl std::fmt::Display) -> String {
    format!("ERR {msg}\n")
}
fn ok_toml(t: &Table) -> String {
    ok_with(&toml::to_string(t).unwrap_or_default())
}

// ---------------------------------------------------------------------------
// Socket server
// ---------------------------------------------------------------------------

/// Bind the control socket (unlinking any stale one, 0600) and serve one
/// request per connection until the runtime tears down.
pub async fn serve(path: PathBuf, ctx: SharedCtx) -> Result<tokio::task::JoinHandle<()>> {
    if let Some(dir) = path.parent()
        && !dir.as_os_str().is_empty()
    {
        std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    }
    if let Err(e) = std::fs::remove_file(&path)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        return Err(e).with_context(|| format!("unlink stale {}", path.display()));
    }
    let listener = UnixListener::bind(&path).with_context(|| format!("bind {}", path.display()))?;
    set_mode_600(&path)?;
    tracing::info!(target: "walshadow::control", socket = %path.display(), "control socket listening");
    Ok(tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let ctx = ctx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_conn(stream, &ctx).await {
                            tracing::debug!(target: "walshadow::control", error = %e, "connection errored");
                        }
                    });
                }
                Err(e) => tracing::warn!(target: "walshadow::control", error = %e, "accept failed"),
            }
        }
    }))
}

async fn handle_conn(mut stream: UnixStream, ctx: &SharedCtx) -> std::io::Result<()> {
    // EOF framing allows newlines in TOML values
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await?;
    let resp = dispatch(&buf, ctx).await;
    stream.write_all(resp.as_bytes()).await?;
    stream.flush().await?;
    let _ = stream.shutdown().await;
    Ok(())
}

async fn dispatch(buf: &[u8], ctx: &SharedCtx) -> String {
    let req = match Request::parse(buf) {
        Ok(r) => r,
        Err(e) => return err(format!("{e:#}")),
    };
    let res: Result<String> = match req.verb {
        "apply" => apply(ctx, &req).await,
        "unset" => unset(ctx, &req).await,
        "reload" => ctx.reloader.reload().await.map(|dbs| ok_note(&dbs)),
        "show" => config_show(ctx).await,
        "status" => stream_status(ctx).await,
        "tables" => tables_list(ctx, &req).await,
        "schemas" => schemas_list(ctx, &req).await,
        "columns" => columns_list(ctx, &req).await,
        other => Err(anyhow::anyhow!("unknown command {other}")),
    };
    res.unwrap_or_else(|e| err(format!("{e:#}")))
}

// ---- handlers -------------------------------------------------------------

/// Keeps invalid fragments from breaking reloads or later starts
async fn apply(ctx: &SharedCtx, req: &Request<'_>) -> Result<String> {
    if req.config.is_empty() {
        bail!("empty apply (send a TOML fragment as the body)");
    }
    let _guard = ctx.frag_lock.lock().await;
    let mut root = load(&ctx.fragment).await?;
    crate::ch_emitter::merge_tables(&mut root, req.config.clone());
    commit(ctx, &ctx.fragment, &root).await
}

/// Removes named keys without touching operator-owned base config
async fn unset(ctx: &SharedCtx, req: &Request<'_>) -> Result<String> {
    let _guard = ctx.frag_lock.lock().await;
    let mut root = load(&ctx.fragment).await?;
    apply_mask(&mut root, &req.config);
    commit(ctx, &ctx.fragment, &root).await
}

fn apply_mask(root: &mut Table, mask: &Table) {
    for (k, v) in mask {
        if let Value::Table(sub) = v {
            if let Some(Value::Table(t)) = root.get_mut(k) {
                apply_mask(t, sub);
            }
        } else {
            root.remove(k);
        }
    }
}

/// Persist `root` as fragment `frag` only once merged config accepts it, so
/// file never holds a rejected fragment, not even briefly
async fn commit(ctx: &SharedCtx, frag: &Path, root: &Table) -> Result<String> {
    validate(ctx, frag, root)
        .await
        .context("rejected: merged config invalid")?;
    save(frag, root).await?;
    let unfollowed = ctx.reloader.reload().await?;
    Ok(ok_note(&unfollowed))
}

/// Matches startup validation so accepted fragments remain restart-safe
async fn validate(ctx: &SharedCtx, frag: &Path, root: &Table) -> Result<()> {
    let mut merged = ctx.cli_base.clone();
    crate::ch_emitter::merge_tables(
        &mut merged,
        crate::ch_emitter::load_merged_with(&ctx.ch_config, Some((frag, root))).await?,
    );
    crate::ch_emitter::EmitterConfig::from_table(&merged)
        .map(|_| ())
        .map_err(|e| anyhow::anyhow!("{e}"))
}

pub const DEFAULT_FRAGMENT: &str = "50-api.toml";

/// Plain `.toml` name, so reload picks the fragment up in its lexical slot of
/// the `ch-config.d` directory
pub fn fragment_path(ch_config: &Path, name: &str) -> Result<PathBuf> {
    let plain = Path::new(name).file_name().is_some_and(|n| n == name);
    if !plain || Path::new(name).extension().is_none_or(|e| e != "toml") {
        bail!("control fragment {name:?} must be a .toml file name without directories");
    }
    Ok(ch_config.with_extension("d").join(name))
}

async fn get_config(ctx: &SharedCtx) -> Result<Table> {
    Ok(crate::ch_emitter::load_effective(&ctx.ch_config, ctx.cli_base.clone()).await?)
}

async fn tables_list<'a>(ctx: &SharedCtx, req: &Request<'a>) -> Result<String> {
    let root = get_config(ctx).await?;
    let db = req
        .config
        .get("database")
        .and_then(Value::as_str)
        .unwrap_or_else(|| dbname(&root));
    let client = pg_connect(&root, Some(db)).await?;
    let ns = req.config.get("namespace").and_then(Value::as_str);
    let listed = introspect::tables(&client, ns)
        .await
        .context("list tables")?;
    let selected: ahash::HashSet<(String, String)> =
        selected_tables(&root, db).into_iter().collect();
    let arr = listed
        .into_iter()
        .map(|t| {
            let key = (t.rel.namespace.to_string(), t.rel.name.to_string());
            let mut row = Table::new();
            row.insert("selected".into(), selected.contains(&key).into());
            row.insert(
                "replica_identity".into(),
                Value::String(t.replica_identity.to_string()),
            );
            row.insert("has_row_key".into(), t.has_row_key().into());
            row.insert("namespace".into(), key.0.into());
            row.insert("name".into(), key.1.into());
            Value::Table(row)
        })
        .collect();
    let mut out = Table::new();
    out.insert("tables".into(), Value::Array(arr));
    Ok(ok_toml(&out))
}

async fn schemas_list(ctx: &SharedCtx, req: &Request<'_>) -> Result<String> {
    let root = get_config(ctx).await?;
    let db = req.config.get("database").and_then(Value::as_str);
    let client = pg_connect(&root, db).await?;
    let names: Vec<Value> = introspect::schemas(&client)
        .await
        .context("list schemas")?
        .into_iter()
        .map(Value::String)
        .collect();
    let mut out = Table::new();
    out.insert("schemas".into(), Value::Array(names));
    Ok(ok_toml(&out))
}

async fn columns_list<'a>(ctx: &SharedCtx, req: &Request<'a>) -> Result<String> {
    let (Some(ns), Some(rel)) = (
        req.config.get("namespace").and_then(Value::as_str),
        req.config.get("relname").and_then(Value::as_str),
    ) else {
        bail!("usage: columns list with [config] `namespace = \"..\"`, `relname = \"..\"`");
    };
    let root = get_config(ctx).await?;
    let db = req.config.get("database").and_then(Value::as_str);
    let client = pg_connect(&root, db).await?;
    let arr = introspect::columns(&client, &RelName::new(ns, rel))
        .await
        .context("list columns")?
        .into_iter()
        .map(|c| {
            let mut t = Table::new();
            t.insert("name".into(), c.name.into());
            t.insert("type".into(), c.pg_type.into());
            t.insert("notnull".into(), c.notnull.into());
            Value::Table(t)
        })
        .collect();
    let mut out = Table::new();
    out.insert("columns".into(), Value::Array(arr));
    Ok(ok_toml(&out))
}

/// Default source database for `[table.*]` entries
fn dbname(root: &Table) -> &str {
    root.get("source")
        .and_then(Value::as_table)
        .and_then(|t| t.get("dbname"))
        .and_then(Value::as_str)
        .unwrap_or_default()
}

/// Databases the config names: `[source] dbname` plus every `[database.*]` key
fn databases(root: &Table) -> Vec<String> {
    let mut out = vec![dbname(root).to_string()];
    if let Some(Value::Table(tbl)) = root.get("database") {
        out.extend(tbl.keys().cloned());
    }
    out.retain(|db| !db.is_empty());
    out.sort();
    out.dedup();
    out
}

/// Tables `db` replicates, ie its entries without `replicate = false`
fn selected_tables(root: &Table, db: &str) -> Vec<(String, String)> {
    replicated_entries(root, db)
        .into_iter()
        .map(|(ns, rel, _)| (ns.to_string(), rel.to_string()))
        .collect()
}

fn replicated_entries<'t>(root: &'t Table, db: &str) -> Vec<(&'t str, &'t str, &'t Table)> {
    let mut out = crate::ch_emitter::table_entries_for(root, db);
    out.retain(|(.., block)| block.get("replicate").and_then(Value::as_bool) != Some(false));
    out
}

/// `[namespace.<ns>]` for `[source] dbname`, `[database.<db>.namespace.<ns>]`
/// for the rest
fn namespace_str(root: &Table, db: &str, namespace: &str, key: &str) -> Option<String> {
    let section = if db == dbname(root) {
        root.get("namespace")
    } else {
        root.get("database")
            .and_then(Value::as_table)
            .and_then(|t| t.get(db))
            .and_then(Value::as_table)
            .and_then(|t| t.get("namespace"))
    };
    section
        .and_then(Value::as_table)
        .and_then(|t| t.get(namespace))
        .and_then(Value::as_table)
        .and_then(|t| t.get(key))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn status_tables(root: &Table, db: &str, ch_database: &str) -> Vec<Value> {
    replicated_entries(root, db)
        .into_iter()
        .map(|(ns, rel, block)| {
            let block_str = |key: &str| block.get(key).and_then(Value::as_str).map(str::to_owned);
            let database = block_str("target_database")
                .or_else(|| namespace_str(root, db, ns, "target_database"))
                .unwrap_or_else(|| ch_database.to_owned());
            let table = block_str("target_table").unwrap_or_else(|| rel.to_owned());
            let initial_load = block_str("initial_load")
                .or_else(|| namespace_str(root, db, ns, "initial_load"))
                .unwrap_or_else(|| "none".into());
            let mut entry = Table::new();
            entry.insert("source_table".into(), format!("{db}.{ns}.{rel}").into());
            entry.insert(
                "destination_table".into(),
                format!("{database}.{table}").into(),
            );
            entry.insert("initial_load".into(), initial_load.into());
            entry.insert("cdc".into(), true.into());
            Value::Table(entry)
        })
        .collect()
}

async fn stream_status(ctx: &SharedCtx) -> Result<String> {
    let root = get_config(ctx).await?;
    let paused = root
        .get("stream")
        .and_then(Value::as_table)
        .and_then(|t| t.get("paused"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let ch_host = root
        .get("ch")
        .and_then(Value::as_table)
        .and_then(|t| t.get("host"))
        .and_then(Value::as_str)
        .unwrap_or("localhost")
        .to_string();
    let ch_database = root
        .get("ch")
        .and_then(Value::as_table)
        .and_then(|t| t.get("database"))
        .and_then(Value::as_str)
        .unwrap_or("default")
        .to_string();
    // One entry per database, each holding that database's selected tables
    let per_db: Vec<Value> = databases(&root)
        .iter()
        .map(|db| {
            let mut row = Table::new();
            row.insert("dbname".into(), db.clone().into());
            row.insert(
                "tables".into(),
                Value::Array(status_tables(&root, db, &ch_database)),
            );
            Value::Table(row)
        })
        .collect();
    // Flat list stays the primary database's, which is what a single-database
    // deployment reports
    let tables = status_tables(&root, dbname(&root), &ch_database);
    let snap = ctx.metrics.snapshot().await;
    let mut out = Table::new();
    out.insert("paused".into(), paused.into());
    out.insert("dbname".into(), dbname(&root).to_string().into());
    out.insert("databases".into(), Value::Array(per_db));
    out.insert("ch_host".into(), ch_host.into());
    out.insert("ch_database".into(), ch_database.into());
    out.insert("tables".into(), Value::Array(tables));
    out.insert(
        "rows_synced".into(),
        (snap.emitter_rows_total as i64).into(),
    );
    out.insert(
        "backfills_pending".into(),
        (snap.config_backfills_pending as i64).into(),
    );
    out.insert(
        "lag_bytes".into(),
        (snap.shadow_apply_lag_bytes as i64).into(),
    );
    out.insert("lag_seconds".into(), snap.shadow_apply_lag_seconds.into());
    out.insert("uptime_secs".into(), (snap.uptime_seconds as i64).into());
    // `show` reports the configured endpoint; this reports whether the pump
    // reached it, which is what an endpoint move waits on, and which proof
    // stopped it when it did not
    out.insert(
        "source_swap_pending".into(),
        (snap.source_endpoint_swap_pending != 0).into(),
    );
    out.insert(
        "source_swap_blocked_on".into(),
        snap.source_endpoint_swap_blocked_on.into(),
    );
    // Preserve unsigned 64-bit system identifier as a string
    out.insert(
        "source_system_id".into(),
        snap.source_system_id.to_string().into(),
    );
    for (key, tli) in [
        ("source_timeline", snap.source_timeline),
        ("floor_timeline", snap.floor_timeline),
        ("shadow_served_timeline", snap.shadow_served_timeline),
        ("shadow_replay_timeline", snap.shadow_replay_timeline),
    ] {
        out.insert(key.into(), i64::from(tli).into());
    }
    // A crossing the pump parked on. The daemon stays up and readable rather
    // than exiting into a restart that re-crosses and re-fails the same way
    out.insert(
        "crossing_blocked_on".into(),
        snap.crossing_blocked_on.into(),
    );
    out.insert(
        "crossing_detail".into(),
        snap.crossing_detail.clone().into(),
    );
    // Step 5's gate, off the endpoint the pump holds: whether the target may be
    // promoted, and which term says no
    out.insert("pause_refrozen".into(), snap.pause_refrozen.into());
    out.insert("promotion_ready".into(), snap.promotion_ready.into());
    out.insert(
        "promotion_blocked_on".into(),
        snap.promotion_blocked_on.into(),
    );
    out.insert(
        "target_in_recovery".into(),
        snap.promotion_target_in_recovery.into(),
    );
    for (key, lsn) in [
        ("pause_consumed_lsn", snap.pause_consumed_lsn),
        ("pause_received_lsn", snap.pause_received_lsn),
        ("target_replay_lsn", snap.promotion_target_replay_lsn),
        ("target_receive_lsn", snap.promotion_target_receive_lsn),
        ("floor", snap.floor_lsn.get()),
        ("source_received", snap.source_received_lsn.get()),
        ("drain", snap.decoder_commit_lsn.get()),
        // Live ack, not the manifest's floored `emitter_ack`
        ("emitter_ack", snap.emitter_ack_lsn.get()),
        ("shadow_replay", snap.shadow_replay_lsn.get()),
    ] {
        out.insert(key.into(), format_pg_lsn(lsn).to_string().into());
    }
    Ok(ok_toml(&out))
}

async fn config_show(ctx: &SharedCtx) -> Result<String> {
    let mut root = get_config(ctx).await?;
    for s in ["source", "ch"] {
        if let Some(Value::Table(sec)) = root.get_mut(s)
            && let Some(p) = sec.get_mut("password")
        {
            *p = Value::String("***".into());
        }
    }
    Ok(ok_with(&toml::to_string(&root).unwrap_or_default()))
}

// ---- TOML file + postgres helpers -----------------------------------------

async fn load(path: &Path) -> Result<Table> {
    match tokio::fs::read_to_string(path).await {
        Ok(s) => s
            .parse::<Table>()
            .with_context(|| format!("parse {}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Table::new()),
        Err(e) => Err(e).with_context(|| format!("read {}", path.display())),
    }
}

async fn save(path: &Path, root: &Table) -> Result<()> {
    let (Some(dir), Some(name)) = (path.parent(), path.file_name().and_then(|n| n.to_str())) else {
        bail!("bad fragment path {}", path.display());
    };
    tokio::fs::create_dir_all(dir).await.ok();
    crate::fs::write_atomic(
        dir,
        name,
        toml::to_string(root).context("serialize toml")?.as_bytes(),
    )
    .await
    .with_context(|| format!("write {}", path.display()))
}

// TODO: use daemon catalog rather than a second connection per request
/// `dbname` names one of the config's databases; `None` takes `[source] dbname`
async fn pg_connect(root: &Table, dbname: Option<&str>) -> Result<Client> {
    let conn = SourceConn::from_table(root).map_err(|e| anyhow::anyhow!("[source] {e}"))?;
    if conn.host.is_empty() {
        bail!("source host not set");
    }
    let mut cfg = conn.to_pg_config();
    if let Some(db) = dbname {
        cfg.database = db.to_string();
    }
    open_sql_client(&cfg)
        .await
        .with_context(|| format!("connect source {}", conn.endpoint()))
}

// ---- misc -----------------------------------------------------------------
fn set_mode_600(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("chmod 600 {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scalar at `[section] key`, for asserting fragment edits
    fn str_at(root: &Table, section: &str, key: &str) -> String {
        root.get(section)
            .and_then(Value::as_table)
            .and_then(|t| t.get(key))
            .map(|v| match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            })
            .unwrap_or_default()
    }

    fn cfg(toml: &str) -> Table {
        if toml.is_empty() {
            Table::new()
        } else {
            toml.parse().unwrap()
        }
    }

    /// Scope `status` and `tables list` to one source database
    #[test]
    fn selected_tables_resolve_the_database_key_level() {
        let root = cfg("[source]\ndbname = \"app\"\n\
             [table.public.orders]\nreplicate = true\n\
             [database.app.table.public.invoices]\nreplicate = true\n\
             [database.app.table.public.audit]\nreplicate = false\n\
             [database.billing.table.public.ledger]\nreplicate = true\n");
        let mut got = selected_tables(&root, "app");
        got.sort();
        assert_eq!(
            got,
            vec![
                ("public".to_string(), "invoices".to_string()),
                ("public".to_string(), "orders".to_string()),
            ]
        );
        assert_eq!(
            selected_tables(&root, "billing"),
            vec![("public".to_string(), "ledger".to_string())],
            "prefixed entries belong to their own database"
        );
        assert_eq!(databases(&root), vec!["app", "billing"]);
        // Invalid config selects no tables here
        assert!(
            selected_tables(&Table::new(), "app").is_empty(),
            "no [table.*]"
        );
        assert!(
            selected_tables(&cfg("table = 3\n"), "app").is_empty(),
            "table is not a table"
        );
        assert!(
            selected_tables(&cfg("[table.public]\norders = 3\n"), "app").is_empty(),
            "entry is not a table"
        );
        assert!(databases(&cfg("table = 3\n")).is_empty(), "no dbname");
    }

    #[test]
    fn request_parse() {
        // TOML must preserve scalar types and quoted delimiters
        let doc = encode_request(
            "apply",
            cfg("[ch]\nhost = \"db\"\nport = 5432\npassword = \"p a$$=w\""),
        )
        .unwrap();
        let r = Request::parse(doc.as_bytes()).unwrap();
        assert_eq!(r.verb, "apply");
        let ch = r.config.get("ch").and_then(Value::as_table).unwrap();
        assert_eq!(ch.get("host").and_then(Value::as_str), Some("db"));
        assert_eq!(ch.get("port").and_then(Value::as_integer), Some(5432));
        assert_eq!(ch.get("password").and_then(Value::as_str), Some("p a$$=w"));

        assert!(Request::parse(b"").is_err());
        let r = Request::parse(b"status").unwrap();
        assert_eq!(r.verb, "status");
        assert!(r.config.is_empty());
    }

    #[test]
    fn apply_mask_removes_and_recurses() {
        let mut root = cfg(
            "[source]\nhost = \"h\"\npassword = \"p\"\n[table.demo.a]\nreplicate = true\n[table.demo.b]\nreplicate = true\n",
        );
        apply_mask(&mut root, &cfg("[source]\npassword = \"\""));
        assert_eq!(str_at(&root, "source", "host"), "h");
        assert!(root["source"].as_table().unwrap().get("password").is_none());
        apply_mask(&mut root, &cfg("[table.demo]\na = \"\"\nmissing = \"\""));
        let demo = root["table"].as_table().unwrap()["demo"]
            .as_table()
            .unwrap();
        assert!(demo.get("a").is_none() && demo.get("b").is_some());
        apply_mask(&mut root, &cfg("table = \"\""));
        assert!(root.get("table").is_none());
    }

    fn ctx_at(dir: &Path) -> SharedCtx {
        SharedCtx {
            ch_config: dir.join("ch-config.toml"),
            fragment: fragment_path(&dir.join("ch-config.toml"), DEFAULT_FRAGMENT).unwrap(),
            cli_base: Table::new(),
            metrics: MetricsRegistry::new(),
            reloader: Arc::new(Reloader::default()),
            frag_lock: Arc::new(Mutex::new(())),
        }
    }

    async fn call(sock: &Path, verb: &str, config: &str) -> String {
        let doc = encode_request(verb, cfg(config)).unwrap();
        let mut s = UnixStream::connect(sock).await.unwrap();
        s.write_all(doc.as_bytes()).await.unwrap();
        s.shutdown().await.unwrap();
        let mut r = String::new();
        s.read_to_string(&mut r).await.unwrap();
        r
    }

    /// A reload cannot add a database: shadow registers its workers at startup
    #[tokio::test]
    async fn reload_reports_databases_it_cannot_follow() {
        const DOC: &str = "[ch]\nhost = \"ch\"\n[source]\ndbname = \"app\"\n\
             [table.public.orders]\nreplicate = true\n\
             [database.billing.table.public.ledger]\nreplicate = true\n";
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ch-config.toml");
        tokio::fs::write(&path, DOC).await.unwrap();
        let doc: Table = DOC.parse().unwrap();
        let base = crate::ch_emitter::EmitterConfig::for_database(&doc, "app").unwrap();
        let (resolver, _rx) = crate::config::ConfigResolver::new(
            &base,
            crate::config::CliOverrides::default(),
            Some(path),
            Table::new(),
            crate::mapping::mapping_handle(Default::default()),
        );
        let reloader = Reloader::default();
        reloader.set_resolvers(vec![resolver]).await;
        assert_eq!(
            reloader.reload().await.unwrap(),
            vec!["billing".to_string()],
            "`app` is followed, `billing` needs a restart"
        );
        assert!(
            ok_note(&["billing".to_string()]).starts_with("OK config names billing"),
            "ctl prints the note on the OK line"
        );
    }

    #[tokio::test]
    async fn apply_show_status_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("c.sock");
        let _h = serve(sock.clone(), ctx_at(dir.path())).await.unwrap();

        assert!(
            call(
                &sock,
                "apply",
                "[ch]\nhost = \"ch\"\nport = 9000\ndatabase = \"demo\"\n[stream]\npaused = true"
            )
            .await
            .starts_with("OK")
        );
        // Keep operator-owned base config untouched
        assert!(!dir.path().join("ch-config.toml").exists());
        assert!(dir.path().join("ch-config.d/50-api.toml").exists());

        let shown = call(&sock, "show", "").await;
        assert!(shown.contains("host = \"ch\""), "{shown}");
        assert!(shown.contains("paused = true"), "{shown}");

        assert!(
            call(&sock, "apply", "[ch]\npassword = \"secret\"")
                .await
                .starts_with("OK")
        );
        let shown = call(&sock, "show", "").await;
        assert!(shown.contains("password = \"***\""), "{shown}");
        assert!(!shown.contains("secret"), "{shown}");

        let status = call(&sock, "status", "").await;
        assert!(status.contains("paused = true"), "{status}");
        let parsed: Table = status.strip_prefix("OK\n").unwrap().parse().unwrap();
        assert_eq!(parsed.get("paused").and_then(Value::as_bool), Some(true));
        assert!(call(&sock, "bogus", "").await.starts_with("ERR"));
        assert!(call(&sock, "apply", "").await.starts_with("ERR"));
    }

    #[tokio::test]
    async fn status_tables_name_both_ends_and_both_phases() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("c.sock");
        let _h = serve(sock.clone(), ctx_at(dir.path())).await.unwrap();

        assert!(
            call(
                &sock,
                "apply",
                "[source]\ndbname = \"app\"\n\
                 [ch]\ndatabase = \"cdc\"\n\
                 [namespace.shop]\ntarget_database = \"warehouse\"\n\
                 [table.public.orders]\nreplicate = true\ninitial_load = \"copy\"\n\
                 [table.public.audit]\nreplicate = false\n\
                 [table.shop.items]\nreplicate = true\ntarget_table = \"line_items\"\n"
            )
            .await
            .starts_with("OK")
        );

        let status = call(&sock, "status", "").await;
        let parsed: Table = status.strip_prefix("OK\n").unwrap().parse().unwrap();
        assert_eq!(
            parsed.get("ch_database").and_then(Value::as_str),
            Some("cdc")
        );
        let tables = parsed.get("tables").and_then(Value::as_array).unwrap();
        let entry = |source: &str| -> Table {
            tables
                .iter()
                .filter_map(Value::as_table)
                .find(|t| t.get("source_table").and_then(Value::as_str) == Some(source))
                .cloned()
                .unwrap_or_else(|| panic!("{source} missing from {status}"))
        };
        assert_eq!(tables.len(), 2, "{status}");

        let orders = entry("app.public.orders");
        assert_eq!(
            orders.get("destination_table").and_then(Value::as_str),
            Some("cdc.orders")
        );
        assert_eq!(
            orders.get("initial_load").and_then(Value::as_str),
            Some("copy")
        );
        assert_eq!(orders.get("cdc").and_then(Value::as_bool), Some(true));

        let items = entry("app.shop.items");
        assert_eq!(
            items.get("destination_table").and_then(Value::as_str),
            Some("warehouse.line_items")
        );
        assert_eq!(
            items.get("initial_load").and_then(Value::as_str),
            Some("none")
        );
    }

    // Regression: applying one table used to opt every other table out
    #[tokio::test]
    async fn apply_merges_unset_removes() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("c.sock");
        let base = dir.path().join("ch-config.toml");
        std::fs::write(
            &base,
            "[table.demo.users]\ncolumns = [{ attnum = 1, target = \"id\", type = \"Int64\" }]\n",
        )
        .unwrap();
        let _h = serve(sock.clone(), ctx_at(dir.path())).await.unwrap();
        let frag = dir.path().join("ch-config.d/50-api.toml");

        assert!(
            call(
                &sock,
                "apply",
                "[table.demo.gizmos]\nreplicate = true\ninitial_load = \"copy\""
            )
            .await
            .starts_with("OK")
        );
        let f = std::fs::read_to_string(&frag).unwrap();
        assert!(f.contains("gizmos"), "{f}");
        assert!(
            !f.contains("users"),
            "apply must not touch the pinned users mapping: {f}"
        );
        assert!(f.contains("initial_load = \"copy\""), "{f}");

        assert!(
            call(&sock, "apply", "[table.demo.widgets]\nreplicate = true")
                .await
                .starts_with("OK")
        );
        let f = std::fs::read_to_string(&frag).unwrap();
        assert!(f.contains("gizmos") && f.contains("widgets"), "{f}");

        assert!(
            call(&sock, "unset", "[table.demo]\ngizmos = \"\"")
                .await
                .starts_with("OK")
        );
        let f = std::fs::read_to_string(&frag).unwrap();
        assert!(!f.contains("gizmos") && f.contains("widgets"), "{f}");
        assert!(call(&sock, "unset", "table = \"\"").await.starts_with("OK"));
        assert!(!std::fs::read_to_string(&frag).unwrap().contains("widgets"));
        // Empty unset is a nop, not an error
        assert!(call(&sock, "unset", "").await.starts_with("OK"));
    }

    #[test]
    fn fragment_path_takes_plain_toml_names_only() {
        let base = Path::new("/etc/walshadow/ch-config.toml");
        assert_eq!(
            fragment_path(base, "90-ctl.toml").unwrap(),
            Path::new("/etc/walshadow/ch-config.d/90-ctl.toml")
        );
        for bad in [
            "../90-ctl.toml",
            "sub/90-ctl.toml",
            "/tmp/90-ctl.toml",
            "90-ctl",
            "",
        ] {
            assert!(fragment_path(base, bad).is_err(), "{bad:?}");
        }
    }

    // Supervisor rewrites of the default fragment leave control state alone,
    // and the later name wins where both set a key
    #[tokio::test]
    async fn apply_writes_named_fragment() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("c.sock");
        let confd = dir.path().join("ch-config.d");
        std::fs::create_dir_all(&confd).unwrap();
        std::fs::write(confd.join("50-api.toml"), "[stream]\npaused = false\n").unwrap();
        let mut ctx = ctx_at(dir.path());
        ctx.fragment = fragment_path(&ctx.ch_config, "90-ctl.toml").unwrap();
        let _h = serve(sock.clone(), ctx).await.unwrap();

        assert!(
            call(&sock, "apply", "[stream]\npaused = true")
                .await
                .starts_with("OK")
        );
        assert_eq!(
            std::fs::read_to_string(confd.join("50-api.toml")).unwrap(),
            "[stream]\npaused = false\n"
        );
        let merged = crate::ch_emitter::load_merged_with(&dir.path().join("ch-config.toml"), None)
            .await
            .unwrap();
        assert_eq!(merged["stream"]["paused"].as_bool(), Some(true));

        assert!(
            call(&sock, "unset", "[stream]\npaused = \"\"")
                .await
                .starts_with("OK")
        );
        let merged = crate::ch_emitter::load_merged_with(&dir.path().join("ch-config.toml"), None)
            .await
            .unwrap();
        assert_eq!(merged["stream"]["paused"].as_bool(), Some(false));
    }

    // Invalid fragments must not poison later reloads or starts
    #[tokio::test]
    async fn apply_rejects_invalid_without_writing() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("c.sock");
        let _h = serve(sock.clone(), ctx_at(dir.path())).await.unwrap();
        let frag = dir.path().join("ch-config.d/50-api.toml");

        assert!(
            call(&sock, "apply", "[ch]\nport = 70000")
                .await
                .starts_with("ERR")
        );
        assert!(!frag.exists(), "rejected first apply left a fragment");
        assert!(
            call(&sock, "apply", "[ch]\nhost = \"ch\"\nport = 9000")
                .await
                .starts_with("OK")
        );
        assert!(!frag.with_extension("toml.tmp").exists());
        assert!(
            call(&sock, "apply", "[ch]\nport = 70000")
                .await
                .starts_with("ERR")
        );
        let f = std::fs::read_to_string(&frag).unwrap();
        assert!(f.contains("port = 9000") && !f.contains("70000"), "{f}");
    }

    // Candidate validates in its lexical slot, so a later fragment still wins
    #[tokio::test]
    async fn apply_validates_under_later_fragments() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("c.sock");
        let _h = serve(sock.clone(), ctx_at(dir.path())).await.unwrap();
        let confd = dir.path().join("ch-config.d");
        std::fs::create_dir_all(&confd).unwrap();
        std::fs::write(confd.join("60-op.toml"), "[ch]\nport = 70000\n").unwrap();

        let resp = call(&sock, "apply", "[ch]\nport = 9000").await;
        assert!(resp.starts_with("ERR"), "{resp}");
        assert!(!confd.join("50-api.toml").exists());

        std::fs::write(confd.join("60-op.toml"), "[ch]\nport = 9000\n").unwrap();
        let resp = call(&sock, "apply", "[ch]\nport = 70000").await;
        assert!(resp.starts_with("OK"), "{resp}");
    }
}
