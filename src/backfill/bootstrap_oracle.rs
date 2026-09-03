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
use crate::schema::RelName;

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
        workers: usize,
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
            let dump = run_pg_dump(&source_conninfo, source_password.as_deref())
                .context("pg_dump --binary-upgrade")?;
            a.apply_schema_dump(&dump).context("apply schema")?;
            a.stop().context("stop -b")?;

            let mut bridge = BridgeConf::in_dir(&b_sock);
            bridge.socket_path = b_bridge;
            bridge.library_dir = bridge_lib_dir;
            bridge.workers = workers;
            let cfg_b = oracle_cfg(&b_data, &b_base, &b_sock, Some(bridge));
            let b = Shadow::new(cfg_b);
            b.write_base_conf().context("serve conf")?;
            b.start().context("start serve")?;
            Ok(b)
        })
        .await
        .context("bootstrap oracle provision task")?
        .context("bootstrap oracle provision")?;

        let bridge =
            crate::ops::bridge::connect_with_budget(&bridge_socket, workers, connect_budget)
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

fn run_pg_dump(conninfo: &str, password: Option<&str>) -> Result<String> {
    let mut cmd = Command::new("pg_dump");
    cmd.args([
        "--binary-upgrade",
        "--schema-only",
        "--no-owner",
        "--no-privileges",
        "-d",
        conninfo,
    ]);
    if let Some(pw) = password {
        cmd.env("PGPASSWORD", pw);
    }
    let out = cmd.output().context("spawn pg_dump")?;
    if !out.status.success() {
        anyhow::bail!("pg_dump failed: {}", String::from_utf8_lossy(&out.stderr));
    }
    String::from_utf8(out.stdout).context("pg_dump output not utf8")
}

/// Provisioning costs an `initdb` + `pg_dump` + apply + restart, so gate it
/// on the relations that actually reach ClickHouse. `walked` narrows the
/// mapped set further to what the greenfield snapshot page-walks: an
/// `initial_load = "none"` relation ships no bootstrap row, so its oracle
/// columns are not a reason to stand up a side Postgres
pub fn needs_oracle(
    catalog: &CatalogMap,
    tables: &MappingSnapshot,
    column_rules: &ColumnRules,
    walked: impl Fn(&RelName) -> bool,
) -> bool {
    let alloc = Allocator::stdlib();
    let system = SystemColumns::default();
    catalog
        .descriptors()
        .filter(|d| walked(&d.rel_name))
        .any(|desc| {
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
        assert!(needs_oracle(&catalog, &tables, &rules, |_| true));
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
        assert!(!needs_oracle(&catalog, &tables, &rules, |_| true));
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
        assert!(needs_oracle(&catalog, &tables, &rules, |_| true));
    }

    #[test]
    fn unmapped_relation_needs_no_oracle() {
        let rules = ColumnRules::default();
        let (catalog, _) = bridged(rel(vec![attr(1, "doc", JSONOID, "json", -1)]), &rules);
        assert!(!needs_oracle(&catalog, &Arc::default(), &rules, |_| true));
    }

    #[test]
    fn relation_out_of_the_snapshot_needs_no_oracle() {
        let rules = ColumnRules::default();
        let (catalog, tables) = bridged(
            rel(vec![
                attr(1, "id", INT4OID, "int4", 4),
                attr(2, "doc", JSONOID, "json", -1),
            ]),
            &rules,
        );
        assert!(needs_oracle(&catalog, &tables, &rules, |_| true));
        assert!(!needs_oracle(&catalog, &tables, &rules, |_| false));
    }
}
