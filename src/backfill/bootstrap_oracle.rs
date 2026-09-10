use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};

use clickhouse_c::Allocator;

use crate::backfill::backup_page_walk::CatalogMap;
use crate::catalog::shadow::{BridgeConf, Shadow, ShadowConfig};
use crate::column_rules::ColumnRules;
use crate::emit::ch_emitter::TablePlan;
use crate::mapping::{MappingSnapshot, SystemColumns};
use crate::ops::oracle::Oracle;

const ORACLE_PORT: u16 = 55440;

pub struct BootstrapOracle {
    shadow: Shadow,
    oracle: Arc<Oracle>,
    base_dir: PathBuf,
}

impl BootstrapOracle {
    pub async fn provision(
        base_dir: PathBuf,
        source_conninfo: String,
        source_password: Option<String>,
        bridge_lib_dir: Option<PathBuf>,
        connect_budget: Duration,
    ) -> Result<Self> {
        let data_dir = base_dir.join("pg");
        let socket_dir = base_dir.join("sock");
        let bridge_socket = socket_dir.join("walshadow-bridge.sock");

        let (b_data, b_sock, b_bridge, b_base) = (
            data_dir.clone(),
            socket_dir.clone(),
            bridge_socket.clone(),
            base_dir.clone(),
        );
        let shadow = tokio::task::spawn_blocking(move || -> Result<Shadow> {
            std::fs::remove_dir_all(&b_base).ok();
            std::fs::create_dir_all(&b_sock)?;

            let cfg_a = oracle_cfg(&b_data, &b_base, &b_sock, None);
            let a = Shadow::new(cfg_a);
            a.initdb().context("initdb")?;
            a.write_base_conf().context("base conf")?;
            a.start_binary_upgrade().context("start -b")?;

            let available = a.available_extensions().context("oracle extensions")?;
            let installed = source_extensions(&source_conninfo, source_password.as_deref())
                .context("source extensions")?;
            let unavailable: Vec<String> = installed
                .into_iter()
                .filter(|e| !available.contains(e))
                .collect();

            let major = pg_dump_major().context("pg_dump version")?;
            let dump = if major >= 17 {
                run_pg_dump(&source_conninfo, source_password.as_deref(), &unavailable)
                    .context("pg_dump --binary-upgrade")?
            } else {
                let raw = run_pg_dump(&source_conninfo, source_password.as_deref(), &[])
                    .context("pg_dump --binary-upgrade")?;
                filter_out_extensions(&raw, &unavailable)
            };
            a.apply_schema_dump(&dump).context("apply schema")?;
            a.stop().context("stop -b")?;

            let mut bridge = BridgeConf::in_dir(&b_sock);
            bridge.socket_path = b_bridge;
            bridge.library_dir = bridge_lib_dir;
            let cfg_b = oracle_cfg(&b_data, &b_base, &b_sock, Some(bridge));
            let b = Shadow::new(cfg_b);
            b.write_base_conf().context("serve conf")?;
            b.start().context("start serve")?;
            Ok(b)
        })
        .await
        .context("bootstrap oracle provision task")?
        .context("bootstrap oracle provision")?;

        let bridge = crate::ops::bridge::connect_with_budget(&bridge_socket, connect_budget)
            .await
            .context("bootstrap oracle bridge connect")?;
        Ok(Self {
            shadow,
            oracle: Arc::new(Oracle::new(Arc::new(bridge))),
            base_dir,
        })
    }

    pub fn oracle(&self) -> Arc<Oracle> {
        self.oracle.clone()
    }
}

impl Drop for BootstrapOracle {
    fn drop(&mut self) {
        let _ = self.shadow.stop();
        let _ = std::fs::remove_dir_all(&self.base_dir);
    }
}

fn oracle_cfg(
    data_dir: &std::path::Path,
    filter_out_dir: &std::path::Path,
    socket_dir: &std::path::Path,
    bridge: Option<BridgeConf>,
) -> ShadowConfig {
    let mut cfg = ShadowConfig::new(data_dir.to_path_buf(), filter_out_dir.to_path_buf());
    cfg.socket_dir = socket_dir.to_path_buf();
    cfg.port = ORACLE_PORT;
    cfg.user = "postgres".into();
    cfg.dbname = "postgres".into();
    cfg.bridge = bridge;
    cfg
}

fn run_pg_dump(conninfo: &str, password: Option<&str>, exclude: &[String]) -> Result<String> {
    let mut cmd = Command::new("pg_dump");
    cmd.args([
        "--binary-upgrade",
        "--schema-only",
        "--no-owner",
        "--no-privileges",
    ]);
    for ext in exclude {
        cmd.arg(format!("--exclude-extension={ext}"));
    }
    cmd.args(["-d", conninfo]);
    if let Some(pw) = password {
        cmd.env("PGPASSWORD", pw);
    }
    let out = cmd.output().context("spawn pg_dump")?;
    if !out.status.success() {
        anyhow::bail!("pg_dump failed: {}", String::from_utf8_lossy(&out.stderr));
    }
    String::from_utf8(out.stdout).context("pg_dump output not utf8")
}

/// Extensions installed on the source. The binary-upgrade dump recreates each
/// one's member objects inline, including C functions that load `$libdir/<ext>`.
fn source_extensions(conninfo: &str, password: Option<&str>) -> Result<Vec<String>> {
    let mut cmd = Command::new("psql");
    cmd.args([
        "-tAXq",
        "-d",
        conninfo,
        "-c",
        "SELECT extname FROM pg_catalog.pg_extension",
    ]);
    if let Some(pw) = password {
        cmd.env("PGPASSWORD", pw);
    }
    let out = cmd.output().context("spawn psql")?;
    if !out.status.success() {
        anyhow::bail!(
            "source extensions query failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(String::from_utf8(out.stdout)
        .context("psql output not utf8")?
        .lines()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect())
}

/// Major version of the `pg_dump` on `PATH`. `--exclude-extension` is 17+.
fn pg_dump_major() -> Result<u32> {
    let out = Command::new("pg_dump")
        .arg("--version")
        .output()
        .context("spawn pg_dump --version")?;
    if !out.status.success() {
        anyhow::bail!("pg_dump --version failed");
    }
    let text = String::from_utf8_lossy(&out.stdout);
    text.split_whitespace()
        .find_map(|tok| {
            let digits: String = tok.chars().take_while(char::is_ascii_digit).collect();
            digits.parse::<u32>().ok()
        })
        .with_context(|| format!("parse pg_dump version from {text:?}"))
}

/// Drop, from a `--binary-upgrade` dump, every entry belonging to an
/// `excluded` extension: the extension itself, its `COMMENT`, and each member
/// object (matched by its in-block `ALTER EXTENSION <ext> ADD`). Used on PG16,
/// whose `pg_dump` has no `--exclude-extension`. Top-level `SET` directives are
/// preserved even inside a dropped entry, since they configure the session, not
/// the extension. An entry runs from its `-- Name: …; Type: …` header comment
/// to the next such header.
fn filter_out_extensions(dump: &str, excluded: &[String]) -> String {
    if excluded.is_empty() {
        return dump.to_string();
    }
    let excluded: std::collections::HashSet<&str> = excluded.iter().map(String::as_str).collect();
    let lines: Vec<&str> = dump.lines().collect();
    let n = lines.len();
    let is_header = |i: usize| {
        i + 2 < n
            && lines[i] == "--"
            && lines[i + 1].starts_with("-- Name: ")
            && lines[i + 2] == "--"
    };

    let mut starts = vec![0usize];
    starts.extend((0..n).filter(|&i| is_header(i)));
    starts.dedup();

    let mut out: Vec<&str> = Vec::with_capacity(n);
    for (k, &start) in starts.iter().enumerate() {
        let end = starts.get(k + 1).copied().unwrap_or(n);
        let seg = &lines[start..end];
        if is_header(start) && should_drop_entry(seg, &excluded) {
            out.extend(
                seg.iter()
                    .copied()
                    .filter(|l| l.trim_start().starts_with("SET ")),
            );
        } else {
            out.extend_from_slice(seg);
        }
    }
    let mut joined = out.join("\n");
    joined.push('\n');
    joined
}

fn should_drop_entry(seg: &[&str], excluded: &std::collections::HashSet<&str>) -> bool {
    let meta = seg
        .get(1)
        .and_then(|h| h.strip_prefix("-- Name: "))
        .unwrap_or("");
    if let Some((name, rest)) = meta.split_once("; Type: ") {
        let typ = rest.split(';').next().unwrap_or("").trim();
        if typ == "EXTENSION" && excluded.contains(name) {
            return true;
        }
        if typ == "COMMENT"
            && name
                .strip_prefix("EXTENSION ")
                .is_some_and(|e| excluded.contains(e.trim()))
        {
            return true;
        }
    }
    seg.iter().any(|l| {
        l.trim_start()
            .strip_prefix("ALTER EXTENSION ")
            .and_then(|r| r.split_whitespace().next())
            .is_some_and(|ext| excluded.contains(ext.trim_matches('"')))
    })
}

pub fn needs_oracle(
    catalog: &CatalogMap,
    tables: &MappingSnapshot,
    column_rules: &ColumnRules,
) -> bool {
    let alloc = Allocator::stdlib();
    let system = SystemColumns::default();
    catalog.descriptors().any(|desc| {
        tables.get(&desc.rel_name).is_some_and(|mapping| {
            TablePlan::build(alloc, desc, mapping, column_rules, &system)
                .map_or(true, |plan| plan.needs_oracle())
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_rules::{ColumnRule, ColumnRulesBuilder};
    use crate::decode::heap_decoder::local_matrix_covers;
    use crate::mapping::{TableMapping, TableTarget, derive_columns_for_mapping};
    use crate::schema::{INT4OID, JSONOID, RelAttr, RelDescriptor, RelName, ReplIdent, TEXTOID};
    use crate::table_rules::MatchKind;
    use ahash::HashMap;

    fn attr(attnum: i16, name: &str, type_oid: u32, type_name: &str, type_len: i16) -> RelAttr {
        RelAttr {
            attnum,
            name: name.into(),
            type_oid,
            typmod: -1,
            not_null: false,
            dropped: false,
            type_name: type_name.into(),
            type_byval: type_len > 0,
            type_len,
            type_align: 'i',
            type_storage: if type_len < 0 { 'x' } else { 'p' },
            missing_text: None,
        }
    }

    fn rel(attrs: Vec<RelAttr>) -> RelDescriptor {
        RelDescriptor {
            rfn: walrus::pg::walparser::RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 16385,
            },
            oid: 16385,
            toast_oid: 0,
            namespace_oid: 2200,
            rel_name: RelName::new("public", "foo"),
            kind: 'r',
            persistence: 'p',
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: attrs,
        }
    }

    fn bridged(desc: RelDescriptor, rules: &ColumnRules) -> (CatalogMap, MappingSnapshot) {
        let mapping = TableMapping {
            target: TableTarget::new("default", "foo"),
            columns: derive_columns_for_mapping(&desc, rules),
        };
        let mut tables = HashMap::default();
        tables.insert(desc.rel_name.clone(), mapping);
        let mut catalog = CatalogMap::new();
        catalog.insert(Arc::new(desc));
        (catalog, Arc::new(tables))
    }

    fn override_rule(attname: &str, target_type: &str) -> ColumnRules {
        let mut b = ColumnRulesBuilder::new();
        b.add(
            &RelName::new("public", "foo"),
            MatchKind::Exact,
            attname,
            MatchKind::Exact,
            ColumnRule {
                target_type: Some(target_type.into()),
                ..ColumnRule::default()
            },
        );
        b.finish().0
    }

    #[test]
    fn json_target_needs_oracle_though_source_decodes_locally() {
        assert!(
            local_matrix_covers(JSONOID, -1),
            "premise: json decodes local"
        );
        let rules = ColumnRules::default();
        let (catalog, tables) = bridged(
            rel(vec![
                attr(1, "id", INT4OID, "int4", 4),
                attr(2, "doc", JSONOID, "json", -1),
            ]),
            &rules,
        );
        assert_eq!(
            tables[&RelName::new("public", "foo")].columns[1].target_type,
            "Nullable(JSON)",
            "premise: default bridge maps json to a composite CH target",
        );
        assert!(needs_oracle(&catalog, &tables, &rules));
    }

    #[test]
    fn scalar_targets_need_no_oracle() {
        let rules = ColumnRules::default();
        let (catalog, tables) = bridged(
            rel(vec![
                attr(1, "id", INT4OID, "int4", 4),
                attr(2, "name", TEXTOID, "text", -1),
            ]),
            &rules,
        );
        assert!(!needs_oracle(&catalog, &tables, &rules));
    }

    #[test]
    fn composite_override_over_local_source_needs_oracle() {
        let rules = override_rule("name", "Array(Nullable(String))");
        let (catalog, tables) = bridged(
            rel(vec![
                attr(1, "id", INT4OID, "int4", 4),
                attr(2, "name", TEXTOID, "text", -1),
            ]),
            &rules,
        );
        assert!(needs_oracle(&catalog, &tables, &rules));
    }

    #[test]
    fn unmapped_relation_needs_no_oracle() {
        let rules = ColumnRules::default();
        let (catalog, _) = bridged(rel(vec![attr(1, "doc", JSONOID, "json", -1)]), &rules);
        assert!(!needs_oracle(&catalog, &Arc::default(), &rules));
    }

    #[test]
    fn invalid_target_needs_oracle() {
        let rules = ColumnRules::default();
        let desc = rel(vec![attr(1, "id", INT4OID, "int4", 4)]);
        let (catalog, mut tables) = bridged(desc.clone(), &rules);
        let mapping = Arc::make_mut(&mut tables).get_mut(&desc.rel_name).unwrap();
        mapping.columns[0].target_type = "NotAType".into();
        assert!(
            TablePlan::build(
                Allocator::stdlib(),
                &desc,
                mapping,
                &rules,
                &SystemColumns::default(),
            )
            .is_err()
        );
        assert!(needs_oracle(&catalog, &tables, &rules));
    }

    const DUMP: &str = "\
--
-- PostgreSQL database dump
--

SET statement_timeout = 0;

--
-- Name: pg_clickhouse; Type: EXTENSION; Schema: -; Owner: -
--

DROP EXTENSION IF EXISTS pg_clickhouse;
SELECT pg_catalog.binary_upgrade_create_empty_extension('pg_clickhouse', 'public', true, '1.0', NULL, NULL, ARRAY[]::pg_catalog.text[]);


--
-- Name: EXTENSION pg_clickhouse; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pg_clickhouse IS 'stub clickhouse extension';


--
-- Name: pg_clickhouse_noop(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.pg_clickhouse_noop() RETURNS integer
    LANGUAGE c
    AS '$libdir/pg_clickhouse', 'pg_clickhouse_noop';

-- For binary upgrade, handle extension membership the hard way
ALTER EXTENSION pg_clickhouse ADD FUNCTION public.pg_clickhouse_noop();


SET default_table_access_method = heap;

--
-- Name: app; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.app (
    id integer NOT NULL,
    name text
);
";

    #[test]
    fn filter_drops_only_the_excluded_extension() {
        let out = filter_out_extensions(DUMP, &["pg_clickhouse".to_string()]);
        assert!(
            !out.contains("pg_clickhouse"),
            "all pg_clickhouse refs gone"
        );
        assert!(
            !out.contains("$libdir"),
            "no missing-library CREATE FUNCTION"
        );
        assert!(out.contains("CREATE TABLE public.app"), "mapped table kept");
        assert!(
            out.contains("SET default_table_access_method = heap;"),
            "top-level SET in a dropped entry is preserved"
        );
        assert!(out.contains("SET statement_timeout = 0;"), "preamble kept");
    }

    #[test]
    fn filter_is_identity_when_nothing_excluded() {
        assert_eq!(filter_out_extensions(DUMP, &[]), DUMP.to_string());
    }

    #[test]
    fn filter_keeps_extensions_not_in_the_excluded_set() {
        let out = filter_out_extensions(DUMP, &["some_other_ext".to_string()]);
        assert_eq!(out, DUMP);
    }

    #[test]
    fn filter_extension_entries() {
        for (meta, sql, dropped) in [
            ("stub; Type: EXTENSION", "CREATE EXTENSION stub;", true),
            ("keep; Type: EXTENSION", "CREATE EXTENSION keep;", false),
            (
                "EXTENSION stub; Type: COMMENT",
                "COMMENT ON EXTENSION stub IS 'stub';",
                true,
            ),
            (
                "EXTENSION keep; Type: COMMENT",
                "COMMENT ON EXTENSION keep IS 'keep';",
                false,
            ),
            (
                "stub; Type: COMMENT",
                "COMMENT ON TABLE stub IS 'stub';",
                false,
            ),
            (
                "f(); Type: FUNCTION",
                "  ALTER EXTENSION \"stub\" ADD FUNCTION f();",
                true,
            ),
            (
                "f(); Type: FUNCTION",
                "ALTER EXTENSION keep ADD FUNCTION f();",
                false,
            ),
            ("stub; Type: TABLE", "CREATE TABLE stub (id int);", false),
        ] {
            let entry = format!("--\n-- Name: {meta}; Schema: public; Owner: -\n--\n{sql}\n");
            let settings = "  SET default_table_access_method = heap;\n";
            let tail = "--\n-- Name: app; Type: TABLE; Schema: public; Owner: -\n--\nCREATE TABLE app (id int);\n";
            for prefix in [
                "",
                "-- PostgreSQL database dump\nSET statement_timeout = 0;\n",
            ] {
                let dump = format!("{prefix}{entry}{settings}{tail}");
                let expected = if dropped {
                    format!("{prefix}{settings}{tail}")
                } else {
                    dump.clone()
                };
                assert_eq!(
                    filter_out_extensions(&dump, &["stub".into()]),
                    expected,
                    "{meta}"
                );
            }
        }
    }
}
