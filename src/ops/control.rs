//! In-process control plane: a request/response line protocol over a Unix
//! socket, folded into the daemon (no separate binary, no child process). Every
//! change is a live reload — the daemon streams one session, never restarts.
//!
//! Config is the daemon's own TOML — `[source]`, `[ch]`, `[table.*]`, and
//! `[stream] paused` — merged from `ch-config.toml` + `ch-config.d/`. `set` /
//! `tables select` / `stream stop|start` edit only the API's own fragment
//! (`ch-config.d/50-api.toml`) then `reload`; `get` / `test` / introspection
//! read the merged effective config. Table selection is `[table.<ns>.<rel>]
//! replicate = true`; pause is `[stream] paused = true`. No source-PG writes.
//!
//! Wire protocol (one request per connection): `<noun> <verb> [key=value …]
//! [positional …]`, whitespace-separated. Values are borrowed slices of the
//! line, so a value cannot contain whitespace (v1 limitation; quoting/JSON
//! framing is a future extension). Response: `OK\n` + optional payload, or
//! `ERR <message>\n`. Consumer contract: the `walshadow-peerdb` shim.

use std::collections::HashMap;
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
        *self.resolver.lock().await = r;
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
}

// ---------------------------------------------------------------------------
// Line protocol
// ---------------------------------------------------------------------------

pub struct Request<'a> {
    pub noun: &'a str,
    pub verb: &'a str,
    pub kv: HashMap<&'a str, &'a str>,
    pub positional: Vec<&'a str>,
}

impl<'a> Request<'a> {
    /// Whitespace-split slices of the request line; `key=value` tokens go to
    /// `kv`, bare tokens to `positional`. Values are borrowed slices, so a value
    /// cannot contain whitespace (line-protocol v1 limitation).
    pub fn parse(line: &'a str) -> Option<Request<'a>> {
        let mut it = line.split_whitespace();
        let noun = it.next()?;
        let verb = it.next().unwrap_or_default();
        let mut kv = HashMap::new();
        let mut positional = Vec::new();
        for t in it {
            match t.split_once('=') {
                Some((k, v)) => {
                    kv.insert(k, v);
                }
                None => positional.push(t),
            }
        }
        Some(Request {
            noun,
            verb,
            kv,
            positional,
        })
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
    let mut buf = Vec::with_capacity(512);
    let mut chunk = [0u8; 512];
    loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.contains(&b'\n') || buf.len() > 64 * 1024 {
            break;
        }
    }
    let line = str::from_utf8(&buf).map_err(std::io::Error::other)?;
    let line = line.split('\n').next().unwrap_or("").trim();
    let resp = dispatch(line, ctx).await;
    stream.write_all(resp.as_bytes()).await?;
    stream.flush().await?;
    let _ = stream.shutdown().await;
    Ok(())
}

async fn dispatch(line: &str, ctx: &SharedCtx) -> String {
    let Some(req) = Request::parse(line) else {
        return err("malformed request line");
    };
    // `dest` maps to the TOML `[ch]` section; `source` to `[source]`.
    let res: Result<String> = match (req.noun, req.verb) {
        ("source", "set") => conn_set(ctx, "source", &req).await,
        ("source", "get") => conn_get(ctx, "source").await,
        ("source", "test") => source_test(ctx, &req).await,
        ("dest", "set") => conn_set(ctx, "ch", &req).await,
        ("dest", "get") => conn_get(ctx, "ch").await,
        ("dest", "test") => dest_test(ctx, &req).await,
        ("tables", "list") => tables_list(ctx, &req).await,
        ("tables", "select") => tables_set(ctx, &req, true).await,
        ("tables", "deselect") => tables_set(ctx, &req, false).await,
        ("tables", "clear") => tables_clear(ctx).await,
        ("schemas", "list") => schemas_list(ctx).await,
        ("columns", "list") => columns_list(ctx, &req.positional).await,
        ("stream", "stop") => set_paused(ctx, true).await,
        ("stream", "start") => set_paused(ctx, false).await,
        ("stream", "reload") | ("config", "reload") => ctx.reloader.reload().await.map(|()| ok()),
        ("stream", "status") => stream_status(ctx).await,
        ("config", "show") => config_show(ctx).await,
        (n, v) => Err(anyhow::anyhow!("unknown command {n} {v}")),
    };
    res.unwrap_or_else(|e| err(format!("{e:#}")))
}

// ---- handlers -------------------------------------------------------------

/// Pause/resume is a config flag: write `[stream] paused` into the fragment,
/// then reload so the pump picks it up live.
async fn set_paused(ctx: &SharedCtx, paused: bool) -> Result<String> {
    let frag = frag_path(&ctx.ch_config);
    let mut root = load(&frag).await?;
    section_mut(&mut root, "stream").insert("paused".into(), Value::Boolean(paused));
    save(&frag, &root).await?;
    ctx.reloader.reload().await?;
    Ok(ok())
}

fn frag_path(ch_config: &Path) -> PathBuf {
    ch_config.with_extension("d").join("50-api.toml")
}

async fn get_config(ctx: &SharedCtx) -> Result<Table> {
    Ok(crate::ch_emitter::load_effective(&ctx.ch_config, ctx.source_base.clone()).await?)
}

async fn conn_set(ctx: &SharedCtx, section: &str, req: &Request<'_>) -> Result<String> {
    let frag = frag_path(&ctx.ch_config);
    let mut root = load(&frag).await?;
    let sec = section_mut(&mut root, section);
    for (&k, &v) in &req.kv {
        sec.insert(k.to_string(), coerce(v));
    }
    save(&frag, &root).await?;
    Ok(ok())
}

async fn conn_get(ctx: &SharedCtx, section: &str) -> Result<String> {
    let root = get_config(ctx).await?;
    let mut b = String::new();
    if let Some(Value::Table(sec)) = root.get(section) {
        for (k, v) in sec {
            let shown = if k == "password" {
                "***".to_string()
            } else {
                render(v)
            };
            line(&mut b, k, &shown);
        }
    }
    Ok(ok_with(&b))
}

async fn source_test(ctx: &SharedCtx, req: &Request<'_>) -> Result<String> {
    let root = get_config(ctx).await?;
    pg_connect(&root, &req.kv)
        .await?
        .simple_query("SELECT 1")
        .await?;
    Ok(ok())
}

async fn dest_test(ctx: &SharedCtx, req: &Request<'_>) -> Result<String> {
    let root = get_config(ctx).await?;
    let host = ov(&root, "ch", "host", &req.kv);
    if host.is_empty() {
        bail!("destination host not set");
    }
    let port = {
        let p = ov(&root, "ch", "port", &req.kv);
        if p.is_empty() { "9000".into() } else { p }
    };
    let addr = format!("{host}:{port}");
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        tokio::net::TcpStream::connect(&addr),
    )
    .await
    .map_err(|_| anyhow::anyhow!("connect timeout to {addr}"))?
    .map_err(|e| anyhow::anyhow!("connect {addr}: {e}"))?;
    Ok(ok())
}

async fn tables_list(ctx: &SharedCtx, req: &Request<'_>) -> Result<String> {
    let root = get_config(ctx).await?;
    let client = pg_connect(&root, &HashMap::new()).await?;
    let ns = req.positional.first().copied();
    let base = "SELECT n.nspname, c.relname, c.relreplident \
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog','information_schema') \
           AND n.nspname NOT LIKE 'pg\\_%'";
    let rows = match ns {
        Some(ns) => {
            client
                .query(&format!("{base} AND n.nspname=$1 ORDER BY 1,2"), &[&ns])
                .await
        }
        None => client.query(&format!("{base} ORDER BY 1,2"), &[]).await,
    }
    .context("list tables")?;
    let selected: std::collections::HashSet<String> = selected_tables(&root)
        .into_iter()
        .map(|(ns, rel)| format!("{ns}.{rel}"))
        .collect();
    let mut b = String::new();
    use std::fmt::Write;
    for r in rows {
        let ns: String = r.get(0);
        let rel: String = r.get(1);
        let ident: i8 = r.get(2);
        let full = format!("{ns}.{rel}");
        let sel = if selected.contains(&full) {
            "yes"
        } else {
            "no"
        };
        let rif = if ident as u8 == b'f' {
            "full"
        } else {
            "default"
        };
        writeln!(b, "{full}\t{sel}\t{rif}").ok();
    }
    Ok(ok_with(&b))
}

async fn schemas_list(ctx: &SharedCtx) -> Result<String> {
    let root = get_config(ctx).await?;
    let client = pg_connect(&root, &HashMap::new()).await?;
    let rows = client
        .query(
            "SELECT nspname FROM pg_namespace \
             WHERE nspname NOT IN ('pg_catalog','information_schema') \
               AND nspname NOT LIKE 'pg\\_%' ORDER BY 1",
            &[],
        )
        .await
        .context("list schemas")?;
    let mut b = String::new();
    for r in rows {
        b.push_str(&r.get::<_, String>(0));
        b.push('\n');
    }
    Ok(ok_with(&b))
}

async fn columns_list(ctx: &SharedCtx, positional: &[&str]) -> Result<String> {
    let [ns, rel, ..] = positional else {
        bail!("usage: columns list <namespace> <relname>");
    };
    let root = get_config(ctx).await?;
    let client = pg_connect(&root, &HashMap::new()).await?;
    let rows = client
        .query(
            "SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull \
             FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid \
             JOIN pg_namespace n ON n.oid=c.relnamespace \
             WHERE n.nspname=$1 AND c.relname=$2 AND a.attnum>0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            &[ns, rel],
        )
        .await
        .context("list columns")?;
    let mut b = String::new();
    use std::fmt::Write;
    for r in rows {
        let name: String = r.get(0);
        let ty: String = r.get(1);
        let nn: bool = r.get(2);
        writeln!(b, "{name}\t{ty}\t{}", if nn { "notnull" } else { "null" }).ok();
    }
    Ok(ok_with(&b))
}

/// Additive: `select` writes `replicate = true`, `deselect` writes
/// `replicate = false`, for the named tables only — never touching any other
/// table's scope. So opting one table in/out leaves every other opt-in and
/// every operator-pinned base mapping alone. `clear` drops the fragment's
/// whole `[table]` section.
///
/// `select` backfills existing rows by default (`initial_load = copy`); pass
/// `backfill=false` to opt a table in without a snapshot (stream from the
/// opt-in LSN forward only). `deselect` never sets `initial_load`.
async fn tables_set(ctx: &SharedCtx, req: &Request<'_>, replicate: bool) -> Result<String> {
    let selection = &req.positional;
    if selection.is_empty() {
        bail!("no tables given");
    }
    for t in selection {
        if t.split_once('.').is_none() {
            bail!("table {t:?} must be namespace.relname");
        }
    }
    let backfill = req
        .kv
        .get("backfill")
        .map(|v| !matches!(*v, "false" | "0" | "no"))
        .unwrap_or(true);
    let frag = frag_path(&ctx.ch_config);
    let mut root = load(&frag).await?;
    for t in selection {
        let (ns, rel) = t.split_once('.').unwrap();
        set_table_replicate(&mut root, ns, rel, replicate);
        if replicate {
            let block = section_mut(section_mut(section_mut(&mut root, "table"), ns), rel);
            block.insert(
                "initial_load".into(),
                Value::String(if backfill { "copy" } else { "none" }.into()),
            );
        }
    }
    save(&frag, &root).await?;
    Ok(ok())
}

async fn tables_clear(ctx: &SharedCtx) -> Result<String> {
    let frag = frag_path(&ctx.ch_config);
    let mut root = load(&frag).await?;
    root.remove("table");
    save(&frag, &root).await?;
    Ok(ok())
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

fn set_table_replicate(root: &mut Table, ns: &str, rel: &str, v: bool) {
    let tbl = section_mut(root, "table");
    let nst = section_mut(tbl, ns);
    let block = section_mut(nst, rel);
    block.insert("replicate".into(), Value::Boolean(v));
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
    let mut b = String::new();
    line(&mut b, "state", if paused { "paused" } else { "running" });
    line(&mut b, "rows_synced", &snap.emitter_rows_total.to_string());
    line(
        &mut b,
        "backfills_pending",
        &snap.config_backfills_pending.to_string(),
    );
    line(
        &mut b,
        "lag_bytes",
        &snap.shadow_apply_lag_bytes.to_string(),
    );
    line(
        &mut b,
        "lag_seconds",
        &snap.shadow_apply_lag_seconds.to_string(),
    );
    line(&mut b, "uptime_secs", &snap.uptime_secs.to_string());
    Ok(ok_with(&b))
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

fn section_mut<'a>(root: &'a mut Table, name: &str) -> &'a mut Table {
    let entry = root
        .entry(name.to_string())
        .or_insert_with(|| Value::Table(Table::new()));
    match entry {
        Value::Table(t) => t,
        other => {
            *other = Value::Table(Table::new());
            match other {
                Value::Table(t) => t,
                _ => unreachable!(),
            }
        }
    }
}

/// TOML value from a wire string: bool / integer if it parses, else string.
fn coerce(s: &str) -> Value {
    if s == "true" || s == "false" {
        Value::Boolean(s == "true")
    } else if let Ok(i) = s.parse::<i64>() {
        Value::Integer(i)
    } else {
        Value::String(s.to_string())
    }
}

fn render(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn str_at(root: &Table, section: &str, key: &str) -> String {
    match root
        .get(section)
        .and_then(Value::as_table)
        .and_then(|t| t.get(key))
    {
        Some(v) => render(v),
        None => String::new(),
    }
}

/// Value for `section.key`, overridden by `ov[key]` when present (ephemeral test).
fn ov(root: &Table, section: &str, key: &str, over: &HashMap<&str, &str>) -> String {
    over.get(key)
        .map(|v| v.to_string())
        .unwrap_or_else(|| str_at(root, section, key))
}

/// Connect to source PG from `[source]` (NoTls; ephemeral `over` wins). TLS
/// source probes are a v1 limitation — `sslmode` is forwarded to the streamer.
// TODO: remove control's direct PG read access (introspection) — route through
// the daemon's catalog instead.
async fn pg_connect(root: &Table, over: &HashMap<&str, &str>) -> Result<Client> {
    let host = ov(root, "source", "host", over);
    if host.is_empty() {
        bail!("source host not set");
    }
    let mut cfg = tokio_postgres::Config::new();
    cfg.host(&host)
        .port(ov(root, "source", "port", over).parse().unwrap_or(5432))
        .dbname(nonempty(ov(root, "source", "dbname", over), "postgres"))
        .user(nonempty(ov(root, "source", "user", over), "postgres"));
    let pw = ov(root, "source", "password", over);
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

fn line(b: &mut String, k: &str, v: &str) {
    use std::fmt::Write;
    writeln!(b, "{k}={v}").ok();
}
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

    #[test]
    fn request_parse() {
        let r = Request::parse("tables select public.users public.orders").unwrap();
        assert_eq!((r.noun, r.verb), ("tables", "select"));
        assert_eq!(r.positional, vec!["public.users", "public.orders"]);
        let r = Request::parse("source set host=db port=5432").unwrap();
        assert_eq!(r.kv.get("host"), Some(&"db"));
        assert_eq!(r.kv.get("port"), Some(&"5432"));
        assert!(Request::parse("").is_none());
    }

    #[test]
    fn coerce_types() {
        assert_eq!(coerce("5432"), Value::Integer(5432));
        assert_eq!(coerce("true"), Value::Boolean(true));
        assert_eq!(coerce("ch.local"), Value::String("ch.local".into()));
    }

    #[test]
    fn section_edit_roundtrips() {
        let mut root = Table::new();
        section_mut(&mut root, "ch").insert("host".into(), coerce("ch"));
        section_mut(&mut root, "ch").insert("port".into(), coerce("9000"));
        assert_eq!(str_at(&root, "ch", "host"), "ch");
        assert_eq!(str_at(&root, "ch", "port"), "9000");
    }

    async fn call(sock: &Path, req: &str) -> String {
        let mut s = UnixStream::connect(sock).await.unwrap();
        s.write_all(req.as_bytes()).await.unwrap();
        s.write_all(b"\n").await.unwrap();
        s.shutdown().await.unwrap();
        let mut r = String::new();
        s.read_to_string(&mut r).await.unwrap();
        r
    }

    #[tokio::test]
    async fn socket_set_get_status_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("c.sock");
        let ctx = SharedCtx {
            ch_config: dir.path().join("ch-config.toml"),
            source_base: Table::new(),
            metrics: MetricsRegistry::new(),
            reloader: Arc::new(Reloader::default()),
        };
        let _h = serve(sock.clone(), ctx.clone()).await.unwrap();

        assert!(
            call(&sock, "dest set host=ch port=9000 database=demo")
                .await
                .starts_with("OK")
        );
        // base ch-config.toml is never written; the API wrote a conf.d fragment.
        assert!(!dir.path().join("ch-config.toml").exists());
        assert!(dir.path().join("ch-config.d/50-api.toml").exists());
        let got = call(&sock, "dest get").await;
        assert!(got.contains("host=ch"), "{got}");
        assert!(got.contains("port=9000"), "{got}");
        assert!(
            call(&sock, "dest set password=secret")
                .await
                .starts_with("OK")
        );
        assert!(call(&sock, "dest get").await.contains("password=***"));

        assert!(call(&sock, "stream status").await.contains("state=running"));
        // pause is a config flag written to the fragment.
        assert!(call(&sock, "stream stop").await.starts_with("OK"));
        assert!(call(&sock, "stream status").await.contains("state=paused"));

        assert!(call(&sock, "config show").await.contains("\"ch\""));
        assert!(call(&sock, "bogus verb").await.starts_with("ERR"));
    }

    // `select` is additive: it writes `replicate` for the named tables only,
    // never touching an operator-pinned base mapping (regression for the bug
    // where selecting one table wrote `replicate = false` for every other
    // in-scope table, silently opting the pinned `demo.users` out).
    #[tokio::test]
    async fn tables_select_is_additive() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("c.sock");
        let base = dir.path().join("ch-config.toml");
        std::fs::write(
            &base,
            "[table.demo.users]\ncolumns = [{ attnum = 1, target = \"id\", type = \"Int64\" }]\n",
        )
        .unwrap();
        let ctx = SharedCtx {
            ch_config: base,
            source_base: Table::new(),
            metrics: MetricsRegistry::new(),
            reloader: Arc::new(Reloader::default()),
        };
        let _h = serve(sock.clone(), ctx.clone()).await.unwrap();
        let frag = dir.path().join("ch-config.d/50-api.toml");

        assert!(
            call(&sock, "tables select demo.gizmos")
                .await
                .starts_with("OK")
        );
        let f = std::fs::read_to_string(&frag).unwrap();
        assert!(f.contains("gizmos"), "{f}");
        assert!(
            !f.contains("users"),
            "select must not touch the pinned users mapping: {f}"
        );
        // select backfills by default.
        assert!(f.contains("initial_load = \"copy\""), "{f}");

        // backfill=false opts in without a snapshot.
        assert!(
            call(&sock, "tables select demo.widgets backfill=false")
                .await
                .starts_with("OK")
        );
        assert!(
            std::fs::read_to_string(&frag)
                .unwrap()
                .contains("initial_load = \"none\"")
        );

        // deselect flips only the named table.
        assert!(
            call(&sock, "tables deselect demo.gizmos")
                .await
                .starts_with("OK")
        );
        assert!(std::fs::read_to_string(&frag).unwrap().contains("false"));

        // clear drops the whole [table] section.
        assert!(call(&sock, "tables clear").await.starts_with("OK"));
        assert!(!std::fs::read_to_string(&frag).unwrap().contains("gizmos"));
    }
}
