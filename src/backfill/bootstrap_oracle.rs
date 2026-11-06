use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::SinkExt;
use tokio_postgres::types::Oid;
use tokio_postgres::{Client, NoTls};

use clickhouse_c::Allocator;

use crate::backfill::backup_page_walk::CatalogMap;
use crate::catalog::shadow::{BridgeConf, Shadow, ShadowConfig};
use crate::column_rules::ColumnRules;
use crate::emit::ch_emitter::TablePlan;
use crate::mapping::{MappingSnapshot, SystemColumns};
use crate::ops::oracle::Oracle;
use crate::schema::FIRST_NORMAL_OBJECT_ID;

pub const ORACLE_PORT: u16 = 55440;
/// Use a distinct bootstrap superuser name to avoid source role conflicts
pub const ORACLE_USER: &str = "walshadow_oracle";
/// Match `FirstUnpinnedObjectId` in PostgreSQL's src/include/access/transam.h
/// Initdb assigns OIDs from here to host locale collations; these vary by host
const FIRST_UNPINNED_OBJECT_ID: u32 = 12000;

pub struct BootstrapOracle {
    // Stop PostgreSQL before bridge connections drop
    _shadow: ShadowGuard,
    oracle: Arc<Oracle>,
}

/// Stop temporary PostgreSQL and remove its directory on drop, including when
/// provisioning fails or is cancelled
struct ShadowGuard {
    shadow: Shadow,
    base_dir: PathBuf,
}

impl Drop for ShadowGuard {
    fn drop(&mut self) {
        let _ = self.shadow.stop();
        let _ = std::fs::remove_dir_all(&self.base_dir);
    }
}

impl BootstrapOracle {
    /// Start temporary PostgreSQL with catalog rows needed to convert `types`
    /// Preserve source OIDs
    pub async fn provision(
        base_dir: PathBuf,
        source: &Client,
        types: &[Oid],
        bridge_lib_dir: Option<PathBuf>,
        workers: usize,
        connect_budget: Duration,
    ) -> Result<Self> {
        let socket_dir = base_dir.join("sock");
        let bridge_socket = socket_dir.join("walshadow-bridge.sock");

        let (b_sock, b_bridge) = (socket_dir.clone(), bridge_socket.clone());
        let shadow = tokio::task::spawn_blocking(move || -> Result<ShadowGuard> {
            std::fs::remove_dir_all(&base_dir).ok();
            std::fs::create_dir_all(&b_sock)?;
            let mut bridge = BridgeConf::in_dir(&b_sock);
            bridge.socket_path = b_bridge;
            bridge.library_dir = bridge_lib_dir;
            bridge.workers = workers;
            let mut cfg = ShadowConfig::new(base_dir.join("pg"), base_dir.clone());
            cfg.socket_dir = b_sock;
            cfg.port = ORACLE_PORT;
            cfg.user = ORACLE_USER.into();
            cfg.dbname = "postgres".into();
            cfg.bridge = Some(bridge);
            let guard = ShadowGuard {
                shadow: Shadow::new(cfg),
                base_dir,
            };
            guard.shadow.initdb().context("initdb")?;
            guard.shadow.write_base_conf().context("base conf")?;
            guard.shadow.start().context("start")?;
            Ok(guard)
        })
        .await
        .context("bootstrap oracle provision task")??;

        seed_catalog(source, &socket_dir, types)
            .await
            .context("seed catalog")?;
        let bridge =
            crate::ops::bridge::connect_with_budget(&bridge_socket, workers, connect_budget)
                .await
                .with_context(|| {
                    format!(
                        "bridge connect ({} of {workers} worker sockets present under {}; \
                     a bridge pool over max_worker_processes registers fewer workers \
                     than the daemon dials)",
                        present_sockets(&socket_dir, workers),
                        socket_dir.display(),
                    )
                })?;
        Ok(Self {
            _shadow: shadow,
            oracle: Arc::new(Oracle::new(Arc::new(bridge))),
        })
    }

    pub fn oracle(&self) -> Arc<Oracle> {
        self.oracle.clone()
    }

    /// Outlives the throwaway PG, so bootstrap's oracle cost keeps rendering
    /// after handoff
    pub fn bridge_stats(&self) -> Arc<crate::ops::bridge::BridgeStats> {
        self.oracle.bridge_stats()
    }
}

fn present_sockets(socket_dir: &Path, workers: usize) -> usize {
    (0..workers)
        .filter(|i| {
            let name = if *i == 0 {
                "walshadow-bridge.sock".to_string()
            } else {
                format!("walshadow-bridge.sock.{i}")
            };
            socket_dir.join(name).exists()
        })
        .count()
}

/// Find types needed by `seeds`: array elements, array types, domain base types,
/// composite fields, range subtypes and multiranges. Include other types from
/// extensions that own any of these types, or that those extensions require.
/// 12000 is `FirstUnpinnedObjectId`
const TYPE_CLOSURE: &str = "\
WITH RECURSIVE ty(oid) AS (
    SELECT unnest($1::oid[])
  UNION
    SELECT d.oid FROM ty
    JOIN pg_catalog.pg_type t ON t.oid = ty.oid
    CROSS JOIN LATERAL (
        SELECT t.typelem
        UNION ALL SELECT t.typarray
        UNION ALL SELECT t.typbasetype
        UNION ALL SELECT a.atttypid FROM pg_catalog.pg_attribute a
            WHERE a.attrelid = t.typrelid AND a.attnum > 0
        UNION ALL SELECT r.rngsubtype FROM pg_catalog.pg_range r WHERE r.rngtypid = t.oid
        UNION ALL SELECT r.rngmultitypid FROM pg_catalog.pg_range r WHERE r.rngtypid = t.oid
        UNION ALL SELECT r.rngtypid FROM pg_catalog.pg_range r WHERE r.rngmultitypid = t.oid
        UNION ALL SELECT m.objid FROM pg_catalog.pg_depend e
            JOIN pg_catalog.pg_depend m ON m.refclassid = e.refclassid
                AND m.classid = 'pg_catalog.pg_type'::regclass
                AND m.deptype = 'e'
                AND (m.refobjid = e.refobjid OR m.refobjid IN (
                    SELECT q.refobjid FROM pg_catalog.pg_depend q
                    WHERE q.classid = e.refclassid AND q.objid = e.refobjid
                        AND q.refclassid = e.refclassid AND q.deptype = 'n'))
            WHERE e.classid = 'pg_catalog.pg_type'::regclass AND e.objid = t.oid
                AND e.refclassid = 'pg_catalog.pg_extension'::regclass AND e.deptype = 'e'
    ) d(oid)
    WHERE d.oid >= 12000
)
SELECT oid FROM ty";

const TYPE_EXTENSIONS: &str = "\
SELECT DISTINCT refobjid FROM pg_catalog.pg_depend
WHERE classid = 'pg_catalog.pg_type'::regclass AND objid = ANY($1)
    AND refclassid = 'pg_catalog.pg_extension'::regclass AND deptype = 'e'";

/// Find btree operator families of range subtypes. Range type cache looks up
/// the subtype comparison function through them
const RANGE_FAMILIES: &str = "\
SELECT DISTINCT c.opcfamily FROM pg_catalog.pg_range r
JOIN pg_catalog.pg_opclass c ON c.oid = r.rngsubopc
WHERE r.rngtypid = ANY($1) AND c.opcfamily >= 12000";

/// Find type support, cast and operator family functions. Include all functions
/// from extensions in `$2` because they may call each other by name
const TYPE_FUNCTIONS: &str = "\
SELECT DISTINCT f FROM (
    SELECT unnest(ARRAY[typinput, typoutput, typreceive, typsend, typmodin, typmodout,
                        typanalyze, typsubscript]::oid[])
        FROM pg_catalog.pg_type WHERE oid = ANY($1)
    UNION ALL SELECT unnest(ARRAY[rngcanonical, rngsubdiff]::oid[])
        FROM pg_catalog.pg_range WHERE rngtypid = ANY($1)
    UNION ALL SELECT castfunc FROM pg_catalog.pg_cast
        WHERE castsource = ANY($1) OR casttarget = ANY($1)
    UNION ALL SELECT objid FROM pg_catalog.pg_depend
        WHERE classid = 'pg_catalog.pg_proc'::regclass AND deptype = 'e'
            AND refclassid = 'pg_catalog.pg_extension'::regclass AND refobjid = ANY($2)
    UNION ALL SELECT amproc::oid FROM pg_catalog.pg_amproc WHERE amprocfamily = ANY($3)
) s(f)
WHERE f >= 12000";

/// Find languages of copied functions. Initdb creates plpgsql after importing
/// host locale collations, so its OID varies by host
const LANGUAGES: &str = "\
SELECT DISTINCT prolang FROM pg_catalog.pg_proc WHERE oid = ANY($1) AND prolang >= 12000";

const LANGUAGE_HANDLERS: &str = "\
SELECT f FROM pg_catalog.pg_language
CROSS JOIN LATERAL unnest(ARRAY[lanplcallfoid, laninline, lanvalidator]::oid[]) f
WHERE oid = ANY($1) AND f >= 12000";

/// Preserve source OIDs with binary COPY, requiring matching PostgreSQL majors
/// Copy catalog rows directly so extension libraries load only when called
async fn seed_catalog(source: &Client, socket_dir: &Path, seeds: &[Oid]) -> Result<()> {
    let mut cfg = tokio_postgres::Config::new();
    cfg.host_path(socket_dir)
        .port(ORACLE_PORT)
        .user(ORACLE_USER)
        .dbname("postgres");
    let (oracle, conn) = cfg.connect(NoTls).await.context("connect oracle")?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let major = "SELECT current_setting('server_version_num')::int / 10000";
    let source_major: i32 = source.query_one(major, &[]).await?.get(0);
    let oracle_major: i32 = oracle.query_one(major, &[]).await?.get(0);
    anyhow::ensure!(
        source_major == oracle_major,
        "oracle PostgreSQL {oracle_major} cannot hold catalog rows of source PostgreSQL {source_major}",
    );

    let seeds: Vec<Oid> = seeds
        .iter()
        .copied()
        .filter(|&oid| oid >= FIRST_UNPINNED_OBJECT_ID)
        .collect();
    let oids = |rows: Vec<tokio_postgres::Row>| rows.iter().map(|r| r.get(0)).collect::<Vec<Oid>>();
    let types = oids(source.query(TYPE_CLOSURE, &[&seeds]).await?);
    let exts = oids(source.query(TYPE_EXTENSIONS, &[&types]).await?);
    let families = oids(source.query(RANGE_FAMILIES, &[&types]).await?);
    let mut procs = oids(
        source
            .query(TYPE_FUNCTIONS, &[&types, &exts, &families])
            .await?,
    );
    let langs = oids(source.query(LANGUAGES, &[&procs]).await?);
    procs.extend(oids(source.query(LANGUAGE_HANDLERS, &[&langs]).await?));

    let types = oid_array(&types);
    let exts = oid_array(&exts);
    let families = oid_array(&families);
    let procs = oid_array(&procs);
    let langs = oid_array(&langs);
    let rowtypes = format!("SELECT typrelid FROM pg_catalog.pg_type WHERE oid = ANY({types})");
    for (catalog, filter) in [
        ("pg_authid", format!("oid >= {FIRST_NORMAL_OBJECT_ID}")),
        ("pg_namespace", "oid >= 12000".into()),
        ("pg_collation", "oid >= 12000".into()),
        ("pg_language", format!("oid = ANY({langs})")),
        ("pg_type", format!("oid = ANY({types})")),
        ("pg_class", format!("oid IN ({rowtypes})")),
        (
            "pg_attribute",
            format!("attnum > 0 AND attrelid IN ({rowtypes})"),
        ),
        ("pg_enum", format!("enumtypid = ANY({types})")),
        ("pg_range", format!("rngtypid = ANY({types})")),
        ("pg_opfamily", format!("oid = ANY({families})")),
        ("pg_opclass", format!("opcfamily = ANY({families})")),
        ("pg_amproc", format!("amprocfamily = ANY({families})")),
        ("pg_proc", format!("oid = ANY({procs})")),
        (
            "pg_cast",
            format!(
                "oid >= 12000 \
                 AND (castsource = ANY({types}) OR casttarget = ANY({types}))"
            ),
        ),
        ("pg_extension", format!("oid = ANY({exts})")),
        (
            "pg_depend",
            format!(
                "deptype = 'e' AND refclassid = 'pg_catalog.pg_extension'::regclass \
                 AND refobjid = ANY({exts})"
            ),
        ),
    ] {
        copy_catalog(source, &oracle, catalog, &filter)
            .await
            .with_context(|| format!("copy {catalog}"))?;
    }
    Ok(())
}

/// Inline OIDs because COPY cannot bind parameters
fn oid_array(oids: &[Oid]) -> String {
    let list: Vec<String> = oids.iter().map(Oid::to_string).collect();
    format!("'{{{}}}'::pg_catalog.oid[]", list.join(","))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Transfer {
    Binary,
    Text,
    Null,
}

/// Stage source rows, delete oracle rows they conflict with on any unique index,
/// then insert. Conflicts come from objects initdb creates after bootstrap,
/// whose OIDs vary by host, or from a source that recreated `public`
async fn copy_catalog(source: &Client, oracle: &Client, catalog: &str, filter: &str) -> Result<()> {
    let rel = format!("pg_catalog.{catalog}");
    // Use NULL for types that reject binary input; conversion ignores these fields.
    // Send vectors as text because binary input rejects an empty oidvector
    let columns: Vec<(String, String, Transfer)> = oracle
        .query(
            "SELECT a.attname::text, pg_catalog.format_type(a.atttypid, a.atttypmod),
                    CASE WHEN a.atttypid = ANY(ARRAY['pg_node_tree', 'anyarray', 'aclitem',
                                                     'aclitem[]']::pg_catalog.regtype[])
                         THEN 'null'
                         WHEN a.atttypid = ANY(ARRAY['oidvector', 'int2vector']
                                               ::pg_catalog.regtype[])
                         THEN 'text'
                         ELSE 'binary' END
             FROM pg_catalog.pg_attribute a
             WHERE a.attrelid = $1::text::pg_catalog.regclass AND a.attnum > 0
                 AND NOT a.attisdropped
             ORDER BY a.attnum",
            &[&rel],
        )
        .await?
        .iter()
        .map(|row| {
            let transfer = match row.get::<_, &str>(2) {
                "null" => Transfer::Null,
                "text" => Transfer::Text,
                _ => Transfer::Binary,
            };
            (row.get(0), row.get(1), transfer)
        })
        .collect();
    let keys: Vec<Vec<String>> = oracle
        .query(
            "SELECT array_agg(a.attname::text ORDER BY k.n)
             FROM pg_catalog.pg_index i
             CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY k(attnum, n)
             JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
             WHERE i.indrelid = $1::text::pg_catalog.regclass AND i.indisunique
             GROUP BY i.indexrelid",
            &[&rel],
        )
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect();
    let select = columns
        .iter()
        .filter(|(_, _, how)| *how != Transfer::Null)
        .map(|(name, ty, how)| {
            if *how == Transfer::Text {
                return format!("CAST({name} AS text) AS {name}");
            }
            column_override(catalog, name)
                .map_or_else(|| name.clone(), |v| format!("CAST({v} AS {ty}) AS {name}"))
        })
        .collect::<Vec<_>>()
        .join(", ");
    let insert: Vec<String> = columns
        .iter()
        .map(|(name, ty, how)| {
            if *how == Transfer::Null {
                format!("CAST(NULL AS {ty})")
            } else {
                name.clone()
            }
        })
        .collect();

    oracle
        .batch_execute(&format!(
            "CREATE TEMP TABLE ws_stage AS SELECT {select} FROM {rel} WITH NO DATA"
        ))
        .await?;
    // Read role metadata through pg_roles without superuser access; passwords are masked
    let from = if catalog == "pg_authid" {
        "pg_roles"
    } else {
        catalog
    };
    let out = source
        .copy_out(&format!(
            "COPY (SELECT {select} FROM pg_catalog.{from} WHERE {filter}) TO STDOUT (FORMAT binary)"
        ))
        .await?;
    let sink = oracle
        .copy_in("COPY pg_temp.ws_stage FROM STDIN (FORMAT binary)")
        .await?;
    futures::pin_mut!(out, sink);
    sink.as_mut().send_all(&mut out).await?;
    sink.as_mut().finish().await?;

    // Never delete pinned rows, matching `IsPinnedObject`, where `public` is the
    // one unpinned bootstrap object. Rows without OIDs are keyed by copied
    // objects, which are all unpinned
    let unpinned = if catalog == "pg_namespace" {
        "(o.oid >= 12000 OR o.oid = 2200)"
    } else if columns.iter().any(|(name, _, _)| name == "oid") {
        "o.oid >= 12000"
    } else {
        "true"
    };
    let mut sql = String::new();
    for (name, ty, _) in columns.iter().filter(|(_, _, how)| *how == Transfer::Text) {
        sql.push_str(&format!(
            "ALTER TABLE pg_temp.ws_stage ALTER COLUMN {name} TYPE {ty} USING {name}::{ty};\n"
        ));
    }
    for key in &keys {
        let side = |alias: &str| {
            key.iter()
                .map(|c| format!("{alias}.{c}"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        sql.push_str(&format!(
            "DELETE FROM {rel} o USING pg_temp.ws_stage s \
             WHERE ({}) = ({}) AND {unpinned};\n",
            side("o"),
            side("s"),
        ));
    }
    sql.push_str(&format!(
        "INSERT INTO {rel} SELECT {} FROM pg_temp.ws_stage;\nDROP TABLE pg_temp.ws_stage;",
        insert.join(", "),
    ));
    oracle.batch_execute(&sql).await?;
    Ok(())
}

/// Represent table row types as composites so PostgreSQL does not look for
/// storage, constraints, defaults or partition keys that were not copied
fn column_override(catalog: &str, column: &str) -> Option<&'static str> {
    match (catalog, column) {
        ("pg_class", "relkind") => Some("'c'"),
        (
            "pg_class",
            "relam" | "relfilenode" | "reltablespace" | "reltoastrelid" | "relchecks"
            | "relrewrite",
        ) => Some("0"),
        (
            "pg_class",
            "relhasindex"
            | "relhasrules"
            | "relhastriggers"
            | "relhassubclass"
            | "relrowsecurity"
            | "relforcerowsecurity"
            | "relispartition",
        )
        | ("pg_attribute", "atthasdef" | "atthasmissing" | "attnotnull") => Some("false"),
        ("pg_authid", "rolpassword") => Some("NULL"),
        _ => None,
    }
}

/// Collect source column types from mapped tables for oracle setup
pub fn mapped_types(catalog: &CatalogMap, tables: &MappingSnapshot) -> Vec<Oid> {
    let mut types: Vec<Oid> = catalog
        .partitioned()
        .chain(catalog.descriptors())
        .filter(|desc| tables.contains_key(&desc.rel_name))
        .flat_map(|desc| desc.attributes.iter())
        .filter(|attr| !attr.dropped)
        .map(|attr| attr.type_oid)
        .collect();
    types.sort_unstable();
    types.dedup();
    types
}

/// Include every mapped relation, window WAL also emits snapshot opt-outs
pub fn needs_oracle(
    catalog: &CatalogMap,
    tables: &MappingSnapshot,
    column_rules: &ColumnRules,
) -> bool {
    let alloc = Allocator::global(&mimalloc::MiMalloc);
    let system = SystemColumns::default();
    // Partitions encode under their root's mapping and column types
    catalog
        .partitioned()
        .chain(catalog.descriptors())
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
    use crate::schema::{
        INT4OID, JSONBOID, JSONOID, RelAttr, RelDescriptor, RelName, ReplIdent, TEXTOID,
    };
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
            missing_default: None,
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
            ancestors: Vec::new(),
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: attrs,
        }
    }

    fn bridged(desc: RelDescriptor, rules: &ColumnRules) -> (CatalogMap, MappingSnapshot) {
        let mapping = TableMapping {
            target: TableTarget::new("default", "foo"),
            columns: derive_columns_for_mapping(&desc, rules),
            tees: Vec::new(),
        };
        let mut tables = HashMap::default();
        tables.insert(desc.rel_name.clone(), mapping);
        let mut catalog = CatalogMap::new();
        catalog.insert(Arc::new(desc));
        (catalog, Arc::new(crate::mapping::shared(tables)))
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
    fn json_targets_need_no_oracle() {
        let rules = ColumnRules::default();
        for (oid, name) in [(JSONOID, "json"), (JSONBOID, "jsonb")] {
            assert!(
                local_matrix_covers(oid, -1),
                "premise: {name} decodes local"
            );
            let (catalog, tables) = bridged(
                rel(vec![
                    attr(1, "id", INT4OID, "int4", 4),
                    attr(2, "doc", oid, name, -1),
                ]),
                &rules,
            );
            assert_eq!(
                tables[&RelName::new("public", "foo")].columns[1].target_type,
                "Nullable(String)",
                "premise: default bridge maps {name} to a CH String target",
            );
            assert!(!needs_oracle(&catalog, &tables, &rules), "{name}");
        }
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
        let entry = Arc::make_mut(&mut tables).get_mut(&desc.rel_name).unwrap();
        Arc::make_mut(entry).columns[0].target_type = "NotAType".into();
        let mapping = entry.as_ref();
        assert!(
            TablePlan::build(
                Allocator::global(&mimalloc::MiMalloc),
                &desc,
                mapping,
                &rules,
                &SystemColumns::default(),
            )
            .is_err()
        );
        assert!(needs_oracle(&catalog, &tables, &rules));
    }
}
