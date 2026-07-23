//! In-process control plane over a Unix socket
//!
//! TOML bodies preserve config types and let one request update several
//! sections atomically. Mutations only touch `ch-config.d/50-api.toml`, keeping
//! operator-owned config read-only. PeerDB shim consumes this protocol

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls};
use toml::{Table, Value};

use crate::metrics::MetricsRegistry;

/// Holds the running session's resolver so the control socket + SIGHUP can
/// trigger a live `reload()`. The daemon streams one session; there is no
/// start/stop/restart lifecycle — pause is a config flag applied by reload.
#[derive(Default)]
pub struct Reloader {
    resolver: Mutex<Option<Arc<crate::config::ConfigResolver>>>,
}

impl Reloader {
    pub async fn set_resolver(&self, r: Option<Arc<crate::config::ConfigResolver>>) {
        *self.resolver.lock().await = r.clone();
        // Control socket serves before the session wires a resolver;
        // apply/reload in that window persist fragments but republish
        // nothing (reload() below no-ops on None). Sweep once at wiring so
        // file state and published config converge
        if let Some(r) = r
            && let Err(e) = r.reload().await
        {
            tracing::warn!(
                target: "walshadow::control",
                error = %e,
                "config sweep at resolver wiring failed",
            );
        }
    }

    /// Live reconfigure: re-read the merged config + republish. No restart.
    pub async fn reload(&self) -> anyhow::Result<()> {
        let r = self.resolver.lock().await.clone();
        if let Some(r) = r {
            r.reload()
                .await
                .map_err(|e| anyhow::anyhow!("reload: {e}"))?;
        }
        Ok(())
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
    /// CLI-arg `[source]` defaults; the config file overrides them, matching the
    /// daemon's connection resolution (see `ch_emitter::load_effective`).
    pub source_base: Table,
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
        "reload" => ctx.reloader.reload().await.map(|()| ok()),
        "show" => config_show(ctx).await,
        "status" => stream_status(ctx).await,
        "tables" => tables_list(ctx, &req).await,
        "schemas" => schemas_list(ctx).await,
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
    let frag = frag_path(&ctx.ch_config);
    let _guard = ctx.frag_lock.lock().await;
    let prev = tokio::fs::read(&frag).await.ok();
    let mut root = load(&frag).await?;
    crate::ch_emitter::merge_tables(&mut root, req.config.clone());
    save(&frag, &root).await?;
    commit_or_rollback(ctx, &frag, prev).await
}

/// Removes named keys without touching operator-owned base config
async fn unset(ctx: &SharedCtx, req: &Request<'_>) -> Result<String> {
    let frag = frag_path(&ctx.ch_config);
    let _guard = ctx.frag_lock.lock().await;
    let prev = tokio::fs::read(&frag).await.ok();
    let mut root = load(&frag).await?;
    apply_mask(&mut root, &req.config);
    save(&frag, &root).await?;
    commit_or_rollback(ctx, &frag, prev).await
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

/// Restores last valid fragment when validation fails
async fn commit_or_rollback(ctx: &SharedCtx, frag: &Path, prev: Option<Vec<u8>>) -> Result<String> {
    if let Err(e) = validate(ctx).await {
        if let Some(bytes) = prev {
            tokio::fs::write(frag, bytes).await?;
        } else {
            tokio::fs::remove_file(frag).await?;
        }
        return Err(e).context("rejected: merged config invalid");
    }
    ctx.reloader.reload().await?;
    Ok(ok())
}

/// Matches startup validation so accepted fragments remain restart-safe
async fn validate(ctx: &SharedCtx) -> Result<()> {
    let merged = get_config(ctx).await?;
    crate::ch_emitter::EmitterConfig::from_table(&merged)
        .map(|_| ())
        .map_err(|e| anyhow::anyhow!("{e}"))
}

fn frag_path(ch_config: &Path) -> PathBuf {
    ch_config.with_extension("d").join("50-api.toml")
}

async fn get_config(ctx: &SharedCtx) -> Result<Table> {
    Ok(crate::ch_emitter::load_effective(&ctx.ch_config, ctx.source_base.clone()).await?)
}

async fn tables_list<'a>(ctx: &SharedCtx, req: &Request<'a>) -> Result<String> {
    let root = get_config(ctx).await?;
    let client = pg_connect(&root).await?;
    let ns = req.config.get("namespace").and_then(Value::as_str);
    let base = "SELECT n.nspname, c.relname, c.relreplident \
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog','information_schema') \
           AND n.nspname NOT LIKE 'pg\\_%'";
    let rows = if let Some(ns) = ns {
        client
            .query(&format!("{base} AND n.nspname=$1 ORDER BY 1,2"), &[&ns])
            .await
    } else {
        client.query(&format!("{base} ORDER BY 1,2"), &[]).await
    }
    .context("list tables")?;
    let selected: std::collections::HashSet<(String, String)> =
        selected_tables(&root).into_iter().collect();
    let mut arr = Vec::with_capacity(rows.len());
    for r in rows {
        let ns: String = r.get(0);
        let rel: String = r.get(1);
        let ident: i8 = r.get(2);
        let mut t = Table::new();
        t.insert(
            "selected".into(),
            selected.contains(&(ns.clone(), rel.clone())).into(),
        );
        t.insert(
            "replica_identity".into(),
            Value::String((ident as u8 as char).to_string()),
        );
        t.insert("namespace".into(), ns.into());
        t.insert("name".into(), rel.into());
        arr.push(Value::Table(t));
    }
    let mut out = Table::new();
    out.insert("tables".into(), Value::Array(arr));
    Ok(ok_toml(&out))
}

async fn schemas_list(ctx: &SharedCtx) -> Result<String> {
    let root = get_config(ctx).await?;
    let client = pg_connect(&root).await?;
    let rows = client
        .query(
            "SELECT nspname FROM pg_namespace \
             WHERE nspname NOT IN ('pg_catalog','information_schema') \
               AND nspname NOT LIKE 'pg\\_%' ORDER BY 1",
            &[],
        )
        .await
        .context("list schemas")?;
    let names: Vec<Value> = rows.iter().map(|r| r.get::<_, String>(0).into()).collect();
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
    let client = pg_connect(&root).await?;
    let rows = client
        .query(
            "SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull \
             FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid \
             JOIN pg_namespace n ON n.oid=c.relnamespace \
             WHERE n.nspname=$1 AND c.relname=$2 AND a.attnum>0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            &[&ns, &rel],
        )
        .await
        .context("list columns")?;
    let mut arr = Vec::with_capacity(rows.len());
    for r in rows {
        let mut t = Table::new();
        t.insert("name".into(), r.get::<_, String>(0).into());
        t.insert("type".into(), r.get::<_, String>(1).into());
        t.insert("notnull".into(), r.get::<_, bool>(2).into());
        arr.push(Value::Table(t));
    }
    let mut out = Table::new();
    out.insert("columns".into(), Value::Array(arr));
    Ok(ok_toml(&out))
}

/// (namespace, relname) for every `[table.<ns>.<rel>]` block in `root` whose
/// `replicate` isn't `false` (present block = in scope).
fn selected_tables(root: &Table) -> Vec<(String, String)> {
    let mut out = Vec::new();
    if let Some(Value::Table(tbl)) = root.get("table") {
        for (ns, nsv) in tbl {
            if let Value::Table(nst) = nsv {
                for (rel, relv) in nst {
                    if let Value::Table(block) = relv
                        && block.get("replicate").and_then(Value::as_bool) != Some(false)
                    {
                        out.push((ns.clone(), rel.clone()));
                    }
                }
            }
        }
    }
    out
}

async fn stream_status(ctx: &SharedCtx) -> Result<String> {
    let paused = get_config(ctx)
        .await?
        .get("stream")
        .and_then(Value::as_table)
        .and_then(|t| t.get("paused"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let snap = ctx.metrics.snapshot().await;
    let mut out = Table::new();
    out.insert("paused".into(), paused.into());
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
    out.insert("uptime_secs".into(), (snap.uptime_secs as i64).into());
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
    if let Some(dir) = path.parent()
        && !dir.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(dir).await.ok();
    }
    tokio::fs::write(path, toml::to_string(root).context("serialize toml")?)
        .await
        .with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

fn render(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn str_at(root: &Table, section: &str, key: &str) -> String {
    root.get(section)
        .and_then(Value::as_table)
        .and_then(|t| t.get(key))
        .map(render)
        .unwrap_or_default()
}

// TODO: use daemon catalog, direct NoTls connection cannot inspect TLS-only sources
async fn pg_connect(root: &Table) -> Result<Client> {
    let host = str_at(root, "source", "host");
    if host.is_empty() {
        bail!("source host not set");
    }
    let mut cfg = tokio_postgres::Config::new();
    cfg.host(&host)
        .port(str_at(root, "source", "port").parse().unwrap_or(5432))
        .dbname(nonempty(str_at(root, "source", "dbname"), "postgres"))
        .user(nonempty(str_at(root, "source", "user"), "postgres"));
    let pw = str_at(root, "source", "password");
    if !pw.is_empty() {
        cfg.password(&pw);
    }
    let (client, conn) = cfg
        .connect(NoTls)
        .await
        .context("connect source postgres")?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Ok(client)
}

// ---- misc -----------------------------------------------------------------

fn nonempty(v: String, default: &str) -> String {
    if v.is_empty() { default.to_string() } else { v }
}
fn set_mode_600(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("chmod 600 {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(toml: &str) -> Table {
        if toml.is_empty() {
            Table::new()
        } else {
            toml.parse().unwrap()
        }
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
            source_base: Table::new(),
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

    // Invalid fragments must not poison later reloads or starts
    #[tokio::test]
    async fn apply_rejects_and_rolls_back_invalid() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("c.sock");
        let _h = serve(sock.clone(), ctx_at(dir.path())).await.unwrap();
        let frag = dir.path().join("ch-config.d/50-api.toml");

        assert!(
            call(&sock, "apply", "[ch]\nhost = \"ch\"\nport = 9000")
                .await
                .starts_with("OK")
        );
        assert!(
            call(&sock, "apply", "[ch]\nport = 70000")
                .await
                .starts_with("ERR")
        );
        let f = std::fs::read_to_string(&frag).unwrap();
        assert!(f.contains("port = 9000") && !f.contains("70000"), "{f}");
    }
}
