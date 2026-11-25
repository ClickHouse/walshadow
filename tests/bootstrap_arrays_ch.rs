//! BASE_BACKUP and COPY initial loads encode built-in arrays in process,
//! landing as oracle-converted twin columns of the same source do

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::fs;
use std::time::Duration;

use anyhow::{Context, Result, ensure};

/// Source column, local target, and twin target the oracle converts
const TWINS: [(&str, &str, &str, &str); 15] = [
    (
        "i4",
        "int4[]",
        "Array(Nullable(Int32))",
        "Array(Nullable(String))",
    ),
    ("i8", "int8[]", "Array(Int64)", "Array(String)"),
    (
        "b",
        "bool[]",
        "Array(Nullable(Bool))",
        "Array(Nullable(String))",
    ),
    (
        "u",
        "uuid[]",
        "Array(Nullable(UUID))",
        "Array(Nullable(String))",
    ),
    (
        "t",
        "text[]",
        "Array(Nullable(String))",
        "Array(LowCardinality(Nullable(String)))",
    ),
    (
        "bp",
        "char(3)[]",
        "Array(Nullable(String))",
        "Array(LowCardinality(Nullable(String)))",
    ),
    (
        "n",
        "name[]",
        "Array(Nullable(String))",
        "Array(LowCardinality(Nullable(String)))",
    ),
    (
        "j",
        "jsonb[]",
        "Array(Nullable(String))",
        "Array(LowCardinality(Nullable(String)))",
    ),
    (
        "ip",
        "inet[]",
        "Array(Nullable(String))",
        "Array(LowCardinality(Nullable(String)))",
    ),
    (
        "net",
        "cidr[]",
        "Array(Nullable(String))",
        "Array(LowCardinality(Nullable(String)))",
    ),
    (
        "iv",
        "interval[]",
        "Array(Nullable(String))",
        "Array(LowCardinality(Nullable(String)))",
    ),
    (
        "tz",
        "timetz[]",
        "Array(Nullable(String))",
        "Array(LowCardinality(Nullable(String)))",
    ),
    (
        "mac",
        "macaddr[]",
        "Array(Nullable(String))",
        "Array(LowCardinality(Nullable(String)))",
    ),
    (
        "mac8",
        "macaddr8[]",
        "Array(Nullable(String))",
        "Array(LowCardinality(Nullable(String)))",
    ),
    (
        "ct",
        "tid[]",
        "Array(Nullable(String))",
        "Array(LowCardinality(Nullable(String)))",
    ),
];

fn source_sql() -> String {
    let cols: Vec<String> = TWINS
        .iter()
        .map(|(c, pg, _, _)| format!("{c} {pg}"))
        .collect();
    format!(
        "CREATE TABLE public.ab (id int PRIMARY KEY, {});\n\
             INSERT INTO public.ab VALUES \
                (1, '{{1,NULL,-3}}', '{{9000000000}}', '{{t,NULL}}', \
                    '{{00112233-4455-6677-8899-aabbccddeeff,NULL}}', '{{a,\"b,c\",NULL,\"\"}}', \
                    '{{ab,\"  \",NULL}}', '{{nm,NULL}}', \
                    '{{\"{{\\\"a\\\": [1, null, \\\"x,y\\\"]}}\",NULL,\"\\\"s\\\"\",7}}', \
                    '{{10.0.0.1/24,NULL,::1,1.2.3.4}}', '{{10.0.0.0/8,2001:db8::/32}}', \
                    '{{\"1 year 2 mons 3 days 04:05:06.7\",NULL,-00:00:01}}', \
                    '{{01:02:03+05:30,NULL,23:59:59.5-08}}', '{{08:00:2b:01:02:03,NULL}}', \
                    '{{08:00:2b:01:02:03:04:05}}', '{{\"(1,2)\",NULL,\"(4294967295,65535)\"}}'),\
                (2, '{{}}', '{{}}', '{{}}', '{{}}', '{{}}', '{{}}', '{{}}', \
                    '{{}}', '{{}}', '{{}}', '{{}}', '{{}}', '{{}}', '{{}}', '{{}}'),\
                (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, \
                    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),\
                (4, '[5:6]={{1,2}}', '[0:0]={{3}}', NULL, NULL, '[2:3]={{p,q}}', NULL, NULL, \
                    '[0:0]={{\"[]\"}}', '[3:3]={{1.2.3.4}}', NULL, NULL, NULL, NULL, NULL, NULL);\n\
             INSERT INTO public.ab (id, i4, t, j) SELECT 5, \
                ARRAY(SELECT g FROM generate_series(1, 3000) g), \
                ARRAY(SELECT md5(g::text) FROM generate_series(1, 400) g), \
                ARRAY(SELECT jsonb_build_object('k', g, 'v', md5(g::text)) \
                    FROM generate_series(1, 400) g);\n\
             CHECKPOINT;\nSELECT pg_switch_wal();\n",
        cols.join(", ")
    )
}

fn write_config(path: &std::path::Path, ch_port: u16, mode: &str) -> Result<()> {
    let mut body = format!(
        "[ch]\nhost = \"127.0.0.1\"\nport = {ch_port}\ndatabase = \"default\"\n\
         compression = \"lz4\"\n\n[stream]\nreplicate_all = false\n\n\
         [table.\"public\".\"ab\"]\nreplicate = true\ninitial_load = \"{mode}\"\n\
         target_database = \"default\"\ntarget_table = \"ab\"\ncolumns = [\n  \
         {{ attnum = 1, target = \"id\", type = \"Int32\" }},\n"
    );
    for (i, (c, _, local, twin)) in TWINS.iter().enumerate() {
        let attnum = i + 2;
        body.push_str(&format!(
            "  {{ attnum = {attnum}, target = \"{c}\", type = \"{local}\" }},\n  \
             {{ attnum = {attnum}, target = \"{c}_o\", type = \"{twin}\" }},\n"
        ));
    }
    body.push_str("]\n");
    fs::write(path, body).context("write ch-config")
}

fn create_ch_tables(ch: &fx::ChServer) -> Result<()> {
    let cols: Vec<String> = TWINS
        .iter()
        .map(|(c, _, local, twin)| format!("{c} {local}, {c}_o {twin}"))
        .collect();
    ch.query(&format!(
        "CREATE TABLE default.ab (id Int32, {}, \
            _lsn UInt64, _xid UInt32, _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
        cols.join(", ")
    ))?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_loads_encode_arrays_as_oracle_twins() {
    loads_encode_arrays_as_oracle_twins("base_backup");
}

/// COPY keeps jsonb[] in send form, other arrays rebuild stored bodies
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_loads_encode_arrays_as_oracle_twins() {
    loads_encode_arrays_as_oracle_twins("copy");
}

fn loads_encode_arrays_as_oracle_twins(mode: &str) {
    if !fx::tools::requirements_available() {
        return;
    }
    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();
    let source = fx::start_source(&tmp);
    let _src_stop = fx::StopOnDrop { sh: &source };
    source
        .apply_schema_dump(&source_sql())
        .expect("load source workload");

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    create_ch_tables(&ch).expect("create ch tables");
    let ch_config_path = tmp.path().join("ch-config.toml");
    write_config(&ch_config_path, slot.ch_tcp, mode).expect("write ch-config");

    let daemon = fx::DaemonRun::prepare(tmp.path(), slot.metrics).expect("daemon layout");
    let child = daemon
        .spawn(&source, &ch_config_path, slot.walsender, &[])
        .expect("spawn walshadow-stream");
    let guard = fx::ChildGuard::new(child);

    let md5 = source.psql_one("SELECT md5('400')").unwrap();
    let result = (|| -> Result<()> {
        fx::wait_for_listen(daemon.metrics_addr, Duration::from_secs(30))
            .context("daemon metrics endpoint never came up")?;
        fx::wait_for_ch_value(
            &ch,
            "SELECT count() FROM default.ab FINAL WHERE _is_deleted = 0",
            "5",
            Duration::from_secs(90),
        )?;
        let q = |sql: &str| ch.query(sql).unwrap_or_default();
        for (c, _, _, _) in TWINS {
            let differ = q(&format!(
                "SELECT countIf(toString(arrayMap(x -> toString(x), {c})) != toString({c}_o)) \
                 FROM default.ab FINAL"
            ));
            ensure!(differ == "0", "{c}: {differ} rows differ from oracle twin");
        }
        let row = |id: i32, expr: &str| {
            q(&format!(
                "SELECT {expr} FROM default.ab FINAL WHERE id = {id}"
            ))
        };
        let checks = [
            (row(1, "i4, bp"), "[1,NULL,-3]\t['ab','',NULL]".to_owned()),
            (row(3, "i4, t"), "[]\t[]".to_owned()),
            (row(4, "i4, i8, t"), "[1,2]\t[3]\t['p','q']".to_owned()),
            (
                row(5, "length(i4), i4[3000], t[400], length(j), j[400]"),
                format!("3000\t3000\t{md5}\t400\t{{\"k\": 400, \"v\": \"{md5}\"}}"),
            ),
            (
                row(1, "ip"),
                "['10.0.0.1/24',NULL,'::1/128','1.2.3.4/32']".to_owned(),
            ),
        ];
        for (got, want) in checks {
            ensure!(got == want, "got {got:?}, want {want:?}");
        }
        Ok(())
    })();

    fx::finish_daemon(guard, &daemon, result);
}
