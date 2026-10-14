//! Declarative partitions replicate into their root's destination

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::time::Duration;

use walshadow::ch_emitter::EmitterConfig;
use walshadow::mapping::{
    ColumnMapping, DropTableStrategy, NamespaceMapping, SystemColumnNames, TableTarget, Tee,
};
use walshadow::schema::RelName;
use walshadow::table_rules::{MatchKind, PartitionRoute, TableRule};

struct Run {
    source: fx::ClusterGuard,
    shadow: fx::ClusterGuard,
    ch: fx::ChServer,
    _tmp: tempfile::TempDir,
}

impl Run {
    fn ch(&self, sql: &str) -> String {
        self.ch.query(sql).unwrap_or_else(|e| panic!("{sql}: {e}"))
    }

    fn oid(&self, rel: &str) -> String {
        self.source
            .psql_one(&format!("SELECT '{rel}'::regclass::oid"))
            .expect("source oid")
    }
}

const INSTALL_SQL: &str = include_str!("../sql/runtime_config_install.sql");

/// Source state before streaming starts
struct Setup<'a> {
    /// SQL after `CREATE SCHEMA`, present at bootstrap
    schema: &'a str,
    /// Namespace auto-create, else only config rows bring tables into scope
    auto_create: bool,
    mappings: Vec<fx::TableMappingSpec>,
}

const STREAMED: Setup<'static> = Setup {
    schema: "",
    auto_create: true,
    mappings: Vec::new(),
};

async fn start(
    ns: &str,
    stmts: Vec<String>,
    segments: u64,
    tune: impl FnOnce(&mut EmitterConfig),
) -> Result<Run, String> {
    start_in(ns, STREAMED, stmts, segments, tune).await
}

async fn start_in(
    ns: &str,
    setup: Setup<'_>,
    stmts: Vec<String>,
    segments: u64,
    tune: impl FnOnce(&mut EmitterConfig),
) -> Result<Run, String> {
    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();
    let (
        fx::BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        shadow_stream_state,
    ) = fx::bootstrap_clusters(
        &tmp,
        &format!("{INSTALL_SQL}\nCREATE SCHEMA {ns};\n{}\n", setup.schema),
        slot.source,
        slot.shadow,
        slot.walsender,
    )
    .await;
    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    let mut ddl = fx::DdlPipelineArgs {
        drop_table_strategy: Some(DropTableStrategy::Retain),
        config_schema: Some("walshadow".into()),
        ..Default::default()
    };
    ddl.namespaces.insert(
        ns.into(),
        NamespaceMapping {
            target_database: Some("walshadow_test".into()),
            auto_create: setup.auto_create,
            auto_create_name: Some(walshadow::mapping::NameTemplate::parse("$table$").unwrap()),
            ..Default::default()
        },
    );
    let app_name = format!("walshadow-{ns}");
    let mut pipeline = fx::build_pipeline_with(
        fx::BuildPipelineArgs {
            tmp: &tmp,
            source: &source,
            shadow: &shadow,
            shadow_filter_dir: &shadow_filter_dir,
            shadow_stream_state,
            ch_database: "walshadow_test",
            ch_tcp_port: slot.ch_tcp,
            mappings: setup.mappings,
            app_name: &app_name,
            ddl: Some(ddl),
        },
        tune,
    )
    .await;
    let driver = fx::spawn_workload(&source, stmts);
    let pumped = fx::pump_segments_res(&mut pipeline, segments, Duration::from_secs(60)).await;
    let _ = driver.join();
    let shipped = pumped?;
    assert!(shipped >= segments, "expected ≥{segments} segments ({ns})");
    let target = pipeline.stream.dispatched_lsn();
    shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay catches up");
    pipeline.shutdown().await.expect("pipeline drains clean");
    Ok(Run {
        source,
        shadow,
        ch,
        _tmp: tmp,
    })
}

fn skip_gate() -> bool {
    !fx::tools::requirements_available()
}

fn rule(cfg: &mut EmitterConfig, ns: &str, rel: &str, rule: TableRule) {
    cfg.table_entries
        .push((RelName::new(ns, rel), MatchKind::Exact, rule));
}

fn route_self() -> TableRule {
    TableRule {
        route: Some(PartitionRoute::Self_),
        ..TableRule::default()
    }
}

fn excluded() -> TableRule {
    TableRule {
        replicate: Some(false),
        ..TableRule::default()
    }
}

fn live_ids(table: &str) -> String {
    format!(
        "SELECT groupArray(id) FROM \
         (SELECT id FROM walshadow_test.{table} FINAL WHERE _is_deleted = 0 ORDER BY id)"
    )
}

fn root_ddl(ns: &str) -> String {
    format!(
        "CREATE TABLE {ns}.m (id int, region text, v text, PRIMARY KEY (id, region)) \
         PARTITION BY LIST (region)"
    )
}

fn root(ns: &str) -> Vec<String> {
    vec![
        root_ddl(ns),
        format!("CREATE TABLE {ns}.m_eu PARTITION OF {ns}.m FOR VALUES IN ('eu')"),
        format!("CREATE TABLE {ns}.m_us PARTITION OF {ns}.m FOR VALUES IN ('us')"),
    ]
}

const LIVE: &str = "FROM walshadow_test.m FINAL WHERE _is_deleted = 0";
const LIVE_IDS: &str = "SELECT groupArray(id) FROM \
    (SELECT id FROM walshadow_test.m FINAL WHERE _is_deleted = 0 ORDER BY id)";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_rows_land_in_root_destination() {
    if skip_gate() {
        return;
    }
    let ns = "pt_route";
    let stmts = vec![
        root_ddl(ns),
        format!("CREATE TABLE {ns}.m_eu PARTITION OF {ns}.m FOR VALUES IN ('eu')"),
        format!(
            "CREATE TABLE {ns}.m_am PARTITION OF {ns}.m FOR VALUES IN ('us', 'ca') \
             PARTITION BY LIST (region)"
        ),
        format!("CREATE TABLE {ns}.m_us PARTITION OF {ns}.m_am FOR VALUES IN ('us')"),
        format!("CREATE TABLE {ns}.m_ca PARTITION OF {ns}.m_am FOR VALUES IN ('ca')"),
        format!(
            "INSERT INTO {ns}.m VALUES (1, 'eu', 'a'), (2, 'us', 'b'), (3, 'ca', 'c'), (4, 'eu', 'd')"
        ),
        "SELECT pg_switch_wal()".into(),
        format!("UPDATE {ns}.m SET region = 'us' WHERE id = 1"),
        format!("UPDATE {ns}.m SET v = 'b2' WHERE id = 2"),
        format!("DELETE FROM {ns}.m WHERE id = 4"),
        "SELECT pg_switch_wal()".into(),
    ];
    // Pinned root mapping gains the partition column on creation
    let column = |src_attnum, name: &str, ty: &str| ColumnMapping {
        src_attnum,
        target_name: name.into(),
        target_type: ty.into(),
        type_pinned: true,
    };
    let setup = Setup {
        mappings: vec![fx::TableMappingSpec {
            source_table: RelName::new(ns, "m"),
            target_table: TableTarget::new("walshadow_test", "m"),
            columns: vec![
                column(1, "id", "Int32"),
                column(2, "region", "String"),
                column(3, "v", "String"),
            ],
        }],
        ..STREAMED
    };
    let run = start_in(ns, setup, stmts, 2, |_| {})
        .await
        .expect("pipeline runs");
    let _src = fx::StopOnDrop { sh: &run.source };
    let _shd = fx::StopOnDrop { sh: &run.shadow };

    let us = run.oid(&format!("{ns}.m_us"));
    let ca = run.oid(&format!("{ns}.m_ca"));
    assert_eq!(
        run.ch(&format!(
            "SELECT groupArray((id, region, v, _leaf_oid)) FROM (SELECT * {LIVE} ORDER BY id)"
        )),
        format!("[(1,'us','a',{us}),(2,'us','b2',{us}),(3,'ca','c',{ca})]"),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_truncate_tombstones_only_its_rows() {
    if skip_gate() {
        return;
    }
    let ns = "pt_trunc";
    let mut stmts = root(ns);
    stmts.extend([
        format!(
            "CREATE TABLE {ns}.m_am PARTITION OF {ns}.m FOR VALUES IN ('ca', 'mx') \
             PARTITION BY LIST (region)"
        ),
        format!("CREATE TABLE {ns}.m_ca PARTITION OF {ns}.m_am FOR VALUES IN ('ca')"),
        format!("CREATE TABLE {ns}.m_mx PARTITION OF {ns}.m_am FOR VALUES IN ('mx')"),
        format!(
            "INSERT INTO {ns}.m VALUES (1, 'eu', 'a'), (2, 'us', 'b'), (3, 'eu', 'c'), \
             (5, 'ca', 'd'), (6, 'mx', 'e')"
        ),
        "SELECT pg_switch_wal()".into(),
        format!("TRUNCATE {ns}.m_eu"),
        format!("TRUNCATE {ns}.m_ca"),
        format!("INSERT INTO {ns}.m VALUES (4, 'eu', 'after')"),
        "SELECT pg_switch_wal()".into(),
    ]);
    let run = start(ns, stmts, 2, |cfg| rule(cfg, ns, "m_am", route_self()))
        .await
        .expect("pipeline runs");
    let _src = fx::StopOnDrop { sh: &run.source };
    let _shd = fx::StopOnDrop { sh: &run.shadow };

    assert_eq!(run.ch(LIVE_IDS), "[2,4]",);
    assert_eq!(run.ch(&live_ids("m_am")), "[6]", "nested destination");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn detach_and_drop_follow_their_policies() {
    if skip_gate() {
        return;
    }
    let ns = "pt_leave";
    let mut stmts = root(ns);
    stmts.extend([
        format!("CREATE TABLE {ns}.m_ap (id int, region text, v text, PRIMARY KEY (id, region))"),
        format!("ALTER TABLE {ns}.m ATTACH PARTITION {ns}.m_ap FOR VALUES IN ('ap')"),
        format!("INSERT INTO {ns}.m VALUES (1, 'eu', 'a'), (2, 'us', 'b'), (3, 'ap', 'c')"),
        "SELECT pg_switch_wal()".into(),
        format!("ALTER TABLE {ns}.m DETACH PARTITION {ns}.m_eu"),
        format!("ALTER TABLE {ns}.m DETACH PARTITION {ns}.m_ap"),
        format!("INSERT INTO {ns}.m_eu VALUES (10, 'eu', 'standalone')"),
        format!("INSERT INTO {ns}.m_ap VALUES (11, 'ap', 'standalone')"),
        format!("DROP TABLE {ns}.m_us"),
        "SELECT pg_switch_wal()".into(),
    ]);
    let run = start(ns, stmts, 2, |cfg| {
        cfg.detach_strategy = DropTableStrategy::Drop;
        cfg.drop_table_strategy = DropTableStrategy::Warn;
        let tee = |table: &str, engine: &str| Tee {
            table: table.into(),
            engine: Some(engine.into()),
            ..Tee::default()
        };
        rule(
            cfg,
            ns,
            "m",
            TableRule {
                // Lightweight DELETE, and an engine without row removal
                tee: Some(vec![tee("m_log", "MergeTree"), tee("m_null", "Null")]),
                ..TableRule::default()
            },
        );
    })
    .await
    .expect("pipeline runs");
    let _src = fx::StopOnDrop { sh: &run.source };
    let _shd = fx::StopOnDrop { sh: &run.shadow };

    assert_eq!(
        run.ch(LIVE_IDS),
        "[2]",
        "detach drops the partition's rows, drop under warn keeps them",
    );
    assert_eq!(
        run.ch("SELECT groupArray(id) FROM (SELECT id FROM walshadow_test.m_log ORDER BY id)"),
        "[2]",
    );
    assert_eq!(
        run.ch(&live_ids("m_eu")),
        "[10]",
        "detached table replicates on its own",
    );
    assert_eq!(
        run.ch(&live_ids("m_ap")),
        "[11]",
        "table attached and detached again gets a fresh mapping",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn attached_partition_maps_columns_by_name() {
    if skip_gate() {
        return;
    }
    let ns = "pt_attach";
    let stmts = vec![
        format!(
            "CREATE TABLE {ns}.m (id int, gone text, region text, v text, \
             PRIMARY KEY (id, region)) PARTITION BY LIST (region)"
        ),
        format!("ALTER TABLE {ns}.m DROP COLUMN gone"),
        format!("CREATE TABLE {ns}.m_eu PARTITION OF {ns}.m FOR VALUES IN ('eu')"),
        format!("CREATE TABLE {ns}.n (v text, region text, id int, PRIMARY KEY (id, region))"),
        format!("ALTER TABLE {ns}.m ATTACH PARTITION {ns}.n FOR VALUES IN ('us')"),
        format!("INSERT INTO {ns}.m VALUES (1, 'eu', 'x'), (2, 'us', 'y')"),
        "SELECT pg_switch_wal()".into(),
    ];
    let run = start(ns, stmts, 1, |_| {}).await.expect("pipeline runs");
    let _src = fx::StopOnDrop { sh: &run.source };
    let _shd = fx::StopOnDrop { sh: &run.shadow };

    assert_eq!(
        run.ch(&format!(
            "SELECT groupArray((id, region, v)) FROM (SELECT * {LIVE} ORDER BY id)"
        )),
        "[(1,'eu','x'),(2,'us','y')]",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn route_self_keeps_partition_destination() {
    if skip_gate() {
        return;
    }
    let ns = "pt_self";
    let mut stmts = root(ns);
    stmts.extend([
        format!("CREATE TABLE {ns}.m_x PARTITION OF {ns}.m FOR VALUES IN ('x')"),
        format!(
            "CREATE TABLE {ns}.m_am PARTITION OF {ns}.m FOR VALUES IN ('ca') \
             PARTITION BY LIST (region)"
        ),
        format!("CREATE TABLE {ns}.m_ca PARTITION OF {ns}.m_am FOR VALUES IN ('ca')"),
        format!("INSERT INTO {ns}.m VALUES (1, 'eu', 'a'), (2, 'us', 'b'), (3, 'ca', 'c'), (4, 'x', 'd')"),
        "SELECT pg_switch_wal()".into(),
        // Root empties its destination once, partitions own or skip theirs
        format!("TRUNCATE {ns}.m"),
        format!("INSERT INTO {ns}.m VALUES (5, 'eu', 'e'), (6, 'us', 'f'), (7, 'ca', 'g'), (8, 'x', 'h')"),
        format!("ALTER TABLE {ns}.m ADD COLUMN extra int"),
        "SELECT pg_switch_wal()".into(),
        format!("ALTER TABLE {ns}.m DETACH PARTITION {ns}.m_eu"),
        format!("DROP TABLE {ns}.m"),
        "SELECT pg_switch_wal()".into(),
    ]);
    let run = start(ns, stmts, 3, |cfg| {
        rule(cfg, ns, "m_us", route_self());
        rule(cfg, ns, "m_x", excluded());
    })
    .await
    .expect("pipeline runs");
    let _src = fx::StopOnDrop { sh: &run.source };
    let _shd = fx::StopOnDrop { sh: &run.shadow };

    assert_eq!(run.ch(LIVE_IDS), "[5,7]", "detach under retain keeps rows");
    assert_eq!(run.ch(&live_ids("m_us")), "[6]");
    assert_eq!(
        run.ch(
            "SELECT groupArray(table) FROM (SELECT table FROM system.columns \
             WHERE database = 'walshadow_test' AND name = 'extra' ORDER BY table)"
        ),
        "['m','m_eu','m_us']",
        "excluded partition has no destination",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unlogged_partition_stops_replication() {
    if skip_gate() {
        return;
    }
    let ns = "pt_unlogged";
    let stmts = vec![
        format!(
            "CREATE TABLE {ns}.m (id int, region text, PRIMARY KEY (id, region)) \
             PARTITION BY LIST (region)"
        ),
        format!("CREATE UNLOGGED TABLE {ns}.m_eu PARTITION OF {ns}.m FOR VALUES IN ('eu')"),
        "SELECT pg_switch_wal()".into(),
    ];
    let Err(e) = start(ns, stmts, 1, |_| {}).await else {
        panic!("unlogged partition replicated");
    };
    assert!(e.contains("unlogged partition"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn attach_loads_existing_rows_into_root() {
    if skip_gate() {
        return;
    }
    let ns = "pt_load";
    let stmts = vec![
        root_ddl(ns),
        format!("CREATE TABLE {ns}.n (id int, region text, v text, PRIMARY KEY (id, region))"),
        format!("INSERT INTO {ns}.n VALUES (1, 'us', 'before')"),
        "SELECT pg_switch_wal()".into(),
        format!("ALTER TABLE {ns}.m ATTACH PARTITION {ns}.n FOR VALUES IN ('us')"),
        format!("INSERT INTO {ns}.m VALUES (2, 'us', 'after')"),
        format!("CREATE TABLE {ns}.s (id int, region text, v text, PRIMARY KEY (id, region))"),
        format!("INSERT INTO {ns}.s VALUES (3, 'ca', 'before')"),
        format!("ALTER TABLE {ns}.m ATTACH PARTITION {ns}.s FOR VALUES IN ('ca')"),
        format!("INSERT INTO {ns}.m VALUES (4, 'ca', 'after')"),
        format!("CREATE TABLE {ns}.k (id int PRIMARY KEY) PARTITION BY RANGE (id)"),
        format!("CREATE TABLE {ns}.k_1 (id int PRIMARY KEY)"),
        format!("INSERT INTO {ns}.k_1 VALUES (1)"),
        format!("ALTER TABLE {ns}.k ATTACH PARTITION {ns}.k_1 FOR VALUES FROM (0) TO (10)"),
        format!("INSERT INTO {ns}.k VALUES (2)"),
        "SELECT pg_switch_wal()".into(),
    ];
    let run = start(ns, stmts, 2, |cfg| {
        rule(cfg, ns, "s", route_self());
        rule(
            cfg,
            ns,
            "k",
            TableRule {
                initial_load: Some("none".into()),
                ..TableRule::default()
            },
        );
    })
    .await
    .expect("pipeline runs");
    let _src = fx::StopOnDrop { sh: &run.source };
    let _shd = fx::StopOnDrop { sh: &run.shadow };
    let leaf = run.oid(&format!("{ns}.n"));
    fx::wait_query(
        &run.ch,
        &format!("SELECT groupArray((id, v, _leaf_oid)) FROM (SELECT * {LIVE} ORDER BY id)"),
        &format!("[(1,'before',{leaf}),(2,'after',{leaf})]"),
        "attached rows load into root",
    )
    .await;
    assert_eq!(
        run.ch(&live_ids("s")),
        "[3,4]",
        "own destination keeps its rows"
    );
    assert_eq!(
        run.ch(&live_ids("k")),
        "[2]",
        "root's initial_load none skips the load"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partitioned_opt_in_loads_each_partition() {
    if skip_gate() {
        return;
    }
    let ns = "pt_optin";
    let schema = format!(
        "{};
         CREATE TABLE {ns}.m_am PARTITION OF {ns}.m FOR VALUES IN ('ca') PARTITION BY LIST (region);
         CREATE TABLE {ns}.m_ca PARTITION OF {ns}.m_am FOR VALUES IN ('ca');
         INSERT INTO {ns}.m VALUES (1, 'eu', 'a'), (2, 'us', 'b'), (9, 'ca', 'x');
         CREATE TABLE {ns}.u (id int PRIMARY KEY) PARTITION BY RANGE (id);
         CREATE TABLE {ns}.u_1 PARTITION OF {ns}.u FOR VALUES FROM (0) TO (10);\n",
        root(ns).join(";\n")
    );
    let opt_in = |rel: &str, replicate: bool, load: &str| {
        format!(
            "INSERT INTO walshadow.config_table (namespace, relname, replicate, initial_load) \
             VALUES ('{ns}', '{rel}', {replicate}, {load})"
        )
    };
    let stmts = vec![
        opt_in("m_am", false, "NULL"),
        // Partitions load by copy whatever their root asks for
        opt_in("m", true, "'base_backup'"),
        // Partitions take their root's destination, or none below an exclusion
        opt_in("m_eu", true, "NULL"),
        opt_in("m_ca", true, "NULL"),
        format!("INSERT INTO {ns}.m VALUES (3, 'eu', 'c'), (10, 'ca', 'y')"),
        // Out of scope root
        format!("CREATE TABLE {ns}.u_2 PARTITION OF {ns}.u FOR VALUES FROM (10) TO (20)"),
        format!("TRUNCATE {ns}.u_2"),
        format!("DROP TABLE {ns}.u_1"),
        "SELECT pg_switch_wal()".into(),
    ];
    let setup = Setup {
        schema: &schema,
        auto_create: false,
        mappings: Vec::new(),
    };
    let run = start_in(ns, setup, stmts, 1, |cfg| cfg.replicate_all = false)
        .await
        .expect("pipeline runs");
    let _src = fx::StopOnDrop { sh: &run.source };
    let _shd = fx::StopOnDrop { sh: &run.shadow };
    fx::wait_query(&run.ch, LIVE_IDS, "[1,2,3]", "each partition loads").await;
    assert_eq!(
        run.ch("SELECT groupArray(name) FROM system.tables WHERE database = 'walshadow_test'"),
        "['m']",
        "only root has a destination",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_removal_needs_partition_column() {
    if skip_gate() {
        return;
    }
    let ns = "pt_noleaf";
    let mut stmts = root(ns);
    stmts.extend([
        format!("INSERT INTO {ns}.m VALUES (1, 'eu', 'a')"),
        format!("TRUNCATE {ns}.m_eu"),
        "SELECT pg_switch_wal()".into(),
    ]);
    let Err(e) = start(ns, stmts, 1, |cfg| {
        let system = SystemColumnNames {
            leaf: Some(String::new()),
            ..SystemColumnNames::default()
        };
        rule(
            cfg,
            ns,
            "m",
            TableRule {
                system,
                ..TableRule::default()
            },
        );
    })
    .await
    else {
        panic!("partition rows removed without partition column");
    };
    assert!(e.contains("needs the partition column"), "{e}");
}
