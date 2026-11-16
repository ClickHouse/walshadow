//! First-run setup: probe both ends, pick tables, write the config
//!
//! `init` takes two connection URLs, validates both endpoints, reports
//! what the source must fix before replication can start (with the SQL
//! that fixes it), then writes `[source]` / `[ch]` / `[table.*]`, so a
//! first run needs no hand-authored TOML
//!
//! Tables land as opt-in intents (`replicate = true`, no `columns`), so
//! shapes come from the source descriptor and CH tables auto-create —
//! see [`crate::opt_in::apply_table_opt_in`], which boot and reload share

use std::collections::BTreeSet;
use std::io::{BufRead, IsTerminal, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use toml::{Table, Value};

use crate::ch_emitter::EmitterConfig;
use crate::config::SourceConn;
use crate::introspect::{self, SourceTable};
use crate::preflight::{self, MIN_SERVER_VERSION_NUM, PreflightError, SourceInputs};
use crate::schema::RelName;
use crate::source_feed::open_sql_client;

pub const PG_URL_ENV: &str = "WALSHADOW_PG_URL";
pub const CH_URL_ENV: &str = "WALSHADOW_CH_URL";

pub struct InitOpts {
    /// Config to write. Refuses to clobber unless `force`
    pub config: PathBuf,
    pub source_url: Option<String>,
    pub ch_url: Option<String>,
    /// Explicit `(namespace, name)` selection; empty defers to `all_tables`
    /// or the picker
    pub tables: Vec<RelName>,
    pub all_tables: bool,
    /// Restrict listing and `--all-tables` to one schema
    pub namespace: Option<String>,
    /// `none` / `copy` / `base_backup` / `object_store`
    pub initial_load: String,
    pub force: bool,
}

/// Probe, select, write. Returns after the config exists on disk; starting
/// the daemon stays the caller's step
pub async fn run(opts: InitOpts) -> Result<()> {
    if opts.config.exists() && !opts.force {
        bail!(
            "{} exists — pass --force to overwrite, or edit it in place",
            opts.config.display()
        );
    }
    let source_url = resolve_url(
        opts.source_url.clone(),
        PG_URL_ENV,
        "Source Postgres URL",
        "postgres://user:password@host:5432/dbname",
        terminal(),
    )?;
    let source = crate::dsn::source_table(&source_url)?;
    let conn = SourceConn::from_table(&wrap("source", source.clone()))
        .map_err(anyhow::Error::msg)
        .context("[source]")?;

    println!("\nsource {}", conn.endpoint());
    let client = open_sql_client(&conn.to_pg_config())
        .await
        .with_context(|| format!("connect source {}", conn.endpoint()))?;
    let version_num = server_version_num(&client).await?;
    println!("  ✓ reachable, PostgreSQL {}", version_text(version_num));
    let findings = probe_source(&client, version_num, conn.slot.as_deref()).await?;
    for line in &findings {
        println!("{line}");
    }

    let ch_url = resolve_url(
        opts.ch_url.clone(),
        CH_URL_ENV,
        "ClickHouse URL",
        "clickhouse://user:password@host:9000/database",
        terminal(),
    )?;
    let ch = crate::dsn::ch_table(&ch_url)?;
    let ch_cfg = EmitterConfig::from_table(&wrap("ch", ch.clone())).context("[ch]")?;
    println!("\ndestination {}:{}", ch_cfg.conn.host, ch_cfg.conn.port);
    let created_db = crate::ch_ddl::ensure_boot_database(&ch_cfg)
        .await
        .context("connect ClickHouse")?;
    let ch_client = crate::ch::connect_client(&ch_cfg)
        .await
        .context("connect ClickHouse")?;
    // chc-rs returns None only before handshake
    let i = ch_client.server_info().context("ClickHouse handshake")?;
    println!(
        "  ✓ reachable, ClickHouse {}.{}.{}",
        i.version_major, i.version_minor, i.version_patch
    );
    if created_db {
        println!("  ✓ created database {}", ch_cfg.conn.database);
    }

    let listed = introspect::tables(&client, opts.namespace.as_deref())
        .await
        .context("list source tables")?;
    let picked = select_tables(&listed, &opts, terminal())?;
    if picked.is_empty() {
        println!(
            "\nno tables selected — add them later with `walshadow-stream ctl add <schema> <table>`"
        );
    }

    let doc = build_config(source, ch, &picked, &opts.initial_load);
    write_config(&opts.config, &doc)?;

    println!("\nwrote {}", opts.config.display());
    for rel in &picked {
        println!("  {} {}", rel.namespace, rel.name);
    }
    let blocking = findings.iter().any(|f| f.starts_with("  ✗"));
    if blocking {
        println!("\nfix the ✗ items above first — the daemon refuses to start until then");
    }
    println!(
        "\nstart streaming:\n  walshadow-stream --ch-config {} …",
        opts.config.display()
    );
    Ok(())
}

/// Everything `init` can check with an ordinary SQL connection, rendered
/// as `✓` / `✗` lines with the SQL that clears each `✗`
async fn probe_source(
    client: &tokio_postgres::Client,
    version_num: i32,
    slot: Option<&str>,
) -> Result<Vec<String>> {
    let mut out = Vec::new();
    let report = preflight::source(SourceInputs {
        source_version_num: version_num,
        source_sql: client,
        slot,
        ch_config: None,
    })
    .await
    .context("pre-flight")?;
    if report.is_ok() {
        out.push(format!(
            "  ✓ wal_level = logical, server_version_num ≥ {MIN_SERVER_VERSION_NUM}"
        ));
    }
    for e in &report.errors {
        out.push(format!("  ✗ {e}"));
        out.extend(remedy(e).map(|fix| format!("      {fix}")));
    }

    let row = client
        .query_one(
            "SELECT rolsuper OR rolreplication, current_user::text FROM pg_roles \
             WHERE rolname = current_user",
            &[],
        )
        .await
        .context("read replication privilege")?;
    let can_replicate: bool = row.get(0);
    let role: String = row.get(1);
    if can_replicate {
        out.push(format!("  ✓ {role} may start replication"));
    } else {
        out.push(format!("  ✗ {role} lacks REPLICATION"));
        out.push(format!("      ALTER ROLE {role} REPLICATION;"));
    }

    let senders: i32 = client
        .query_one("SELECT current_setting('max_wal_senders')::int", &[])
        .await
        .context("read max_wal_senders")?
        .get(0);
    if senders < 1 {
        out.push("  ✗ max_wal_senders = 0, no walsender slot for the shadow".into());
        out.push("      ALTER SYSTEM SET max_wal_senders = 8;  -- restart required".into());
    }

    if let Some(mismatch) = shadow_major_mismatch(version_num) {
        out.push(format!("  ✗ {mismatch}"));
    }
    Ok(out)
}

/// Source SQL that clears a pre-flight finding, where one exists
fn remedy(e: &PreflightError) -> Option<String> {
    match e {
        PreflightError::WalLevel { .. } => Some(
            "ALTER SYSTEM SET wal_level = logical;  -- restart required (managed PG: set it in the provider console)"
                .into(),
        ),
        PreflightError::SlotMissing { slot } => {
            Some(format!("SELECT pg_create_physical_replication_slot('{slot}');"))
        }
        _ => None,
    }
}

/// Shadow is a physical clone, so its postmaster must be the source's
/// major. The binaries come off `PATH`, so check the ones this host has
fn shadow_major_mismatch(source_version_num: i32) -> Option<String> {
    let source_major = source_version_num / 10_000;
    let out = std::process::Command::new("initdb")
        .arg("--version")
        .output();
    let Ok(out) = out else {
        return Some(format!(
            "no `initdb` on PATH — the shadow needs PostgreSQL {source_major} binaries \
             (image: rebuild with --build-arg PG_MAJOR={source_major})"
        ));
    };
    let text = String::from_utf8_lossy(&out.stdout);
    let local = text
        .split_whitespace()
        .last()
        .and_then(|v| v.split(['.', 'd', 'r', 'b']).next())
        .and_then(|v| v.parse::<i32>().ok());
    match local {
        Some(major) if major == source_major => None,
        Some(major) => Some(format!(
            "shadow binaries are PostgreSQL {major}, source is {source_major}; \
             a basebackup-cloned shadow cannot span majors \
             (image: rebuild with --build-arg PG_MAJOR={source_major})"
        )),
        None => Some(format!(
            "could not read `initdb --version` ({})",
            text.trim()
        )),
    }
}

/// Explicit list, then `--all-tables`, then the interactive picker
fn select_tables(
    listed: &[SourceTable],
    opts: &InitOpts,
    tty: Option<impl BufRead>,
) -> Result<Vec<RelName>> {
    if !opts.tables.is_empty() {
        let known: foldhash::HashSet<&RelName> = listed.iter().map(|t| &t.rel).collect();
        for rel in &opts.tables {
            if !known.contains(rel) {
                bail!("{}.{} not found on source", rel.namespace, rel.name);
            }
        }
        return Ok(opts.tables.clone());
    }
    let replicable: Vec<&SourceTable> = listed.iter().filter(|t| t.has_row_key()).collect();
    if opts.all_tables {
        for t in listed.iter().filter(|t| !t.has_row_key()) {
            println!(
                "  skipping {}.{} — {}",
                t.rel.namespace,
                t.rel.name,
                t.row_key_note()
            );
        }
        return Ok(replicable.iter().map(|t| t.rel.clone()).collect());
    }
    let Some(mut tty) = tty else {
        bail!("no terminal for the table picker — pass --all-tables or --table <schema> <table>");
    };
    print_table_menu(listed);
    // Re-prompt rather than discard the probing work already done
    loop {
        let answer = prompt(
            "\nTables (numbers, ranges, `all`, or empty for none): ",
            &mut tty,
        )?;
        match resolve_picked(listed, &answer) {
            Ok(picked) => return Ok(picked),
            Err(e) => println!("  {e}"),
        }
    }
}

/// One picker answer against the listing: every index must name a table
/// walshadow can replicate
fn resolve_picked(listed: &[SourceTable], answer: &str) -> Result<Vec<RelName>> {
    let mut out = Vec::new();
    for idx in parse_selection(answer, listed.len())? {
        let t = &listed[idx];
        if !t.has_row_key() {
            bail!(
                "{} {} has no row key: {}",
                t.rel.namespace,
                t.rel.name,
                t.row_key_note()
            );
        }
        out.push(t.rel.clone());
    }
    Ok(out)
}

fn print_table_menu(listed: &[SourceTable]) {
    println!("\nsource tables:");
    let ns_width = listed
        .iter()
        .map(|t| t.rel.namespace.len())
        .max()
        .unwrap_or(0);
    let name_width = listed.iter().map(|t| t.rel.name.len()).max().unwrap_or(0);
    for (i, t) in listed.iter().enumerate() {
        // `!` marks what the picker will refuse, so the reason reads as a fix
        let mark = if t.has_row_key() { ' ' } else { '!' };
        println!(
            "  {:>3}. {mark} {:<ns_width$}  {:<name_width$}  {}",
            i + 1,
            t.rel.namespace,
            t.rel.name,
            t.row_key_note(),
        );
    }
}

/// `all`, `2`, `1,4`, `2-5` — 1-based, deduplicated, ordered
fn parse_selection(answer: &str, len: usize) -> Result<Vec<usize>> {
    let answer = answer.trim();
    if answer.is_empty() || answer.eq_ignore_ascii_case("none") {
        return Ok(Vec::new());
    }
    if answer.eq_ignore_ascii_case("all") {
        return Ok((0..len).collect());
    }
    let mut out = BTreeSet::new();
    for part in answer.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (lo, hi) = match part.split_once('-') {
            Some((a, b)) => (index(a, len)?, index(b, len)?),
            None => {
                let i = index(part, len)?;
                (i, i)
            }
        };
        if lo > hi {
            bail!("range {part:?} runs backwards");
        }
        out.extend(lo..=hi);
    }
    Ok(out.into_iter().collect())
}

fn index(raw: &str, len: usize) -> Result<usize> {
    let n: usize = raw
        .trim()
        .parse()
        .with_context(|| format!("{raw:?} is not a number"))?;
    if n < 1 || n > len {
        bail!("{n} out of range 1..={len}");
    }
    Ok(n - 1)
}

fn build_config(source: Table, ch: Table, picked: &[RelName], initial_load: &str) -> Table {
    let mut root = Table::new();
    root.insert("source".into(), Value::Table(source));
    root.insert("ch".into(), Value::Table(ch));
    if picked.is_empty() {
        return root;
    }
    let mut by_ns: std::collections::BTreeMap<&str, Table> = Default::default();
    for rel in picked {
        let mut block = Table::new();
        block.insert("replicate".into(), true.into());
        block.insert("initial_load".into(), initial_load.into());
        by_ns
            .entry(&rel.namespace)
            .or_default()
            .insert(rel.name.to_string(), Value::Table(block));
    }
    let tables = by_ns
        .into_iter()
        .map(|(ns, t)| (ns.to_owned(), Value::Table(t)))
        .collect();
    root.insert("table".into(), Value::Table(tables));
    root
}

/// 0600: the file carries both passwords
fn write_config(path: &Path, doc: &Table) -> Result<()> {
    // Bare file name: parent is "", which create_dir_all accepts
    let dir = path.parent().unwrap_or(Path::new(""));
    std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    let body = format!(
        "# Written by `walshadow-stream init`. Edit freely, or drive it live\n\
         # over the control socket (`walshadow-stream ctl …`), which writes\n\
         # its own fragments beside this file and never rewrites it.\n\n{}",
        toml::to_string(doc).context("serialize config")?
    );
    std::fs::write(path, body).with_context(|| format!("write {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .context(format!("chmod 0600 {}", path.display()))?;
    }
    Ok(())
}

fn wrap(section: &str, body: Table) -> Table {
    let mut root = Table::new();
    root.insert(section.into(), Value::Table(body));
    root
}

/// Flag, then env, then prompt. Env keeps credentials out of `ps` and
/// shell history
fn resolve_url(
    flag: Option<String>,
    env: &str,
    label: &str,
    example: &str,
    tty: Option<impl BufRead>,
) -> Result<String> {
    if let Some(url) = flag.filter(|s| !s.trim().is_empty()) {
        return Ok(url);
    }
    if let Ok(url) = std::env::var(env)
        && !url.trim().is_empty()
    {
        return Ok(url);
    }
    let Some(mut tty) = tty else {
        bail!("no {label}: pass the flag or set {env} (eg {example})");
    };
    let answer = prompt(&format!("{label} [{example}]: "), &mut tty)?;
    if answer.trim().is_empty() {
        bail!("no {label} given");
    }
    Ok(answer.trim().into())
}

/// Prompt answers: stdin, when it is a terminal
fn terminal() -> Option<std::io::StdinLock<'static>> {
    let stdin = std::io::stdin();
    stdin.is_terminal().then_some(stdin.lock())
}

fn prompt(text: &str, tty: &mut impl BufRead) -> Result<String> {
    print!("{text}");
    std::io::stdout().flush()?;
    let mut line = String::new();
    tty.read_line(&mut line)?;
    Ok(line)
}

async fn server_version_num(client: &tokio_postgres::Client) -> Result<i32> {
    let row = client
        .query_one("SELECT current_setting('server_version_num')::int", &[])
        .await
        .context("read server_version_num")?;
    Ok(row.get(0))
}

fn version_text(version_num: i32) -> String {
    format!("{}.{}", version_num / 10_000, version_num % 10_000)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn listed() -> Vec<SourceTable> {
        vec![
            SourceTable {
                rel: RelName::new("public", "users"),
                replica_identity: 'd',
                has_pk: true,
            },
            SourceTable {
                rel: RelName::new("public", "audit"),
                replica_identity: 'd',
                has_pk: false,
            },
        ]
    }

    const NO_TTY: Option<std::io::Empty> = None;

    fn tty(answers: &str) -> Option<std::io::Cursor<&str>> {
        Some(std::io::Cursor::new(answers))
    }

    #[test]
    fn picker_reprompts_until_an_answer_resolves() {
        let picked = select_tables(&listed(), &opts(), tty("2\n9\n1\n")).unwrap();
        assert_eq!(picked, vec![RelName::new("public", "users")]);
        let e = select_tables(&listed(), &opts(), NO_TTY).unwrap_err();
        assert!(e.to_string().contains("no terminal"), "{e}");
    }

    #[test]
    fn url_resolves_flag_env_then_prompt() {
        let url = |flag: Option<&str>, env: &str, tty: Option<std::io::Cursor<&str>>| {
            resolve_url(flag.map(Into::into), env, "URL", "eg", tty).map_err(|e| e.to_string())
        };
        let unset = "WALSHADOW_INIT_TEST_UNSET";
        assert_eq!(url(Some("pg://flag"), unset, None), Ok("pg://flag".into()));
        let path = std::env::var("PATH").unwrap();
        assert_eq!(url(Some(" "), "PATH", None), Ok(path));
        assert_eq!(
            url(None, unset, tty(" pg://typed \n")),
            Ok("pg://typed".into())
        );
        assert_eq!(url(None, unset, tty("\n")), Err("no URL given".into()));
        let e = url(None, unset, None).unwrap_err();
        assert!(
            e.contains("pass the flag or set WALSHADOW_INIT_TEST_UNSET"),
            "{e}"
        );
    }

    #[test]
    fn remedies_only_for_fixable_findings() {
        let wal = PreflightError::WalLevel {
            got: "replica".into(),
            expected: "logical",
        };
        assert!(remedy(&wal).unwrap().contains("wal_level = logical"));
        let slot = PreflightError::SlotMissing { slot: "s".into() };
        assert!(
            remedy(&slot)
                .unwrap()
                .contains("pg_create_physical_replication_slot('s')")
        );
        let old = PreflightError::SourceVersionTooOld { got: 1, min: 2 };
        assert_eq!(remedy(&old), None);
    }

    #[test]
    fn config_writes_into_missing_dirs_owner_only() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("nested/ch-config.toml");
        let doc = build_config(Table::new(), Table::new(), &[], "none");
        write_config(&path, &doc).unwrap();
        let body = std::fs::read_to_string(&path).unwrap();
        assert!(body.starts_with("# Written by `walshadow-stream init`"));
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    /// Unreachable source fails naming its endpoint
    #[tokio::test(flavor = "current_thread")]
    async fn unreachable_source_names_endpoint() {
        let tmp = tempfile::tempdir().unwrap();
        let e = run(InitOpts {
            config: tmp.path().join("ch-config.toml"),
            source_url: Some("postgres://u@127.0.0.1:1/db".into()),
            ..opts()
        })
        .await
        .unwrap_err();
        assert!(format!("{e:#}").contains("connect source"), "{e:#}");
    }

    fn opts() -> InitOpts {
        InitOpts {
            config: "/tmp/walshadow-init-test.toml".into(),
            source_url: None,
            ch_url: None,
            tables: Vec::new(),
            all_tables: false,
            namespace: None,
            initial_load: "copy".into(),
            force: false,
        }
    }

    #[test]
    fn all_tables_skips_keyless_relations() {
        let picked = select_tables(
            &listed(),
            &InitOpts {
                all_tables: true,
                ..opts()
            },
            NO_TTY,
        )
        .unwrap();
        assert_eq!(picked, vec![RelName::new("public", "users")]);
    }

    #[test]
    fn explicit_table_must_exist_on_source() {
        let e = select_tables(
            &listed(),
            &InitOpts {
                tables: vec![RelName::new("public", "ghost")],
                ..opts()
            },
            NO_TTY,
        )
        .unwrap_err();
        assert!(e.to_string().contains("not found on source"), "{e}");
    }

    #[test]
    fn selection_accepts_numbers_ranges_and_all() {
        assert_eq!(parse_selection("all", 3).unwrap(), vec![0, 1, 2]);
        assert_eq!(parse_selection("1,3", 3).unwrap(), vec![0, 2]);
        assert_eq!(parse_selection(" 2-3 ", 3).unwrap(), vec![1, 2]);
        assert_eq!(parse_selection("", 3).unwrap(), Vec::<usize>::new());
        assert_eq!(parse_selection("2,2", 3).unwrap(), vec![1]);
    }

    #[test]
    fn picker_refuses_a_keyless_table_by_name() {
        let e = resolve_picked(&listed(), "1,2").unwrap_err().to_string();
        assert!(e.contains("public audit has no row key"), "{e}");
        assert_eq!(
            resolve_picked(&listed(), "1").unwrap(),
            vec![RelName::new("public", "users")]
        );
    }

    #[test]
    fn selection_rejects_out_of_range_and_backwards() {
        assert!(parse_selection("4", 3).is_err());
        assert!(parse_selection("0", 3).is_err());
        assert!(parse_selection("3-1", 3).is_err());
        assert!(parse_selection("x", 3).is_err());
    }

    #[test]
    fn config_holds_opt_in_blocks_keyed_by_pair() {
        let doc = build_config(
            crate::dsn::source_table("postgres://u@h/d").unwrap(),
            crate::dsn::ch_table("clickhouse://h/db").unwrap(),
            &[RelName::new("public", "users")],
            "copy",
        );
        let rendered = toml::to_string(&doc).unwrap();
        let parsed = EmitterConfig::from_toml_str(&rendered).unwrap();
        assert!(parsed.tables.is_empty(), "opt-in carries no pinned columns");
        let row = parsed
            .table_opt_ins
            .get(&RelName::new("public", "users"))
            .expect("opt-in intent");
        assert_eq!(row.replicate, Some(true));
        assert_eq!(row.initial_load.as_deref(), Some("copy"));
    }

    #[test]
    fn written_config_round_trips_connection_settings() {
        let doc = build_config(
            crate::dsn::source_table("postgres://repl:pw@src:5433/app?sslmode=require").unwrap(),
            crate::dsn::ch_table("clickhouses://u:p@ch/cdc").unwrap(),
            &[],
            "none",
        );
        let conn = SourceConn::from_table(&doc).unwrap();
        assert_eq!(conn.host, "src");
        assert_eq!(conn.port, 5433);
        assert_eq!(conn.dbname, "app");
        let ch = EmitterConfig::from_table(&doc).unwrap();
        assert_eq!(ch.conn.host, "ch");
        assert_eq!(ch.conn.port, 9440);
        assert!(ch.conn.secure);
    }
}
