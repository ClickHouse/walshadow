//! Chunked COPY partitions a table by heap block so a restart repeats one
//! chunk instead of the whole relation.
//!
//! Drill shape:
//!   1. table wide enough to span many 8 KiB pages
//!   2. EXPLAIN one chunk's predicate, expecting a TID range scan; a plain
//!      seq scan per chunk would turn the walk quadratic
//!   3. copy two adjacent ranges, expecting an exact partition of the rows
//!   4. re-copy the first range, expecting its own rows again and nothing
//!      else: a resumed chunk duplicates, never skips

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::sync::Arc;

use tokio::sync::mpsc;
use walrus::pg::walparser::RelFileNode;
use walshadow::backup_page_walk::BackfillTuple;
use walshadow::ch_emitter::EmitterStats;
use walshadow::copy_backfill::{BlockRange, copy_rows_into};
use walshadow::decode::heap_decoder::ColumnValue;
use walshadow::schema::{INT8OID, RelAttr, RelDescriptor, RelName, ReplIdent, TEXTOID};
use walshadow::source_feed::open_sql_client;

const SCHEMA: &str = "s_chunk";
const N_ROWS: i64 = 20_000;
/// Wide enough that the rows span far more than the two blocks copied below
const PAYLOAD: &str = "repeat('x', 200)";

fn attr(attnum: i16, name: &str, type_oid: u32) -> RelAttr {
    RelAttr {
        attnum,
        name: name.into(),
        type_oid,
        typmod: -1,
        not_null: false,
        dropped: false,
        type_name: String::new(),
        type_byval: type_oid == INT8OID,
        type_len: if type_oid == INT8OID { 8 } else { -1 },
        type_align: 'd',
        type_storage: 'p',
        missing_default: None,
    }
}

async fn ids(
    client: &tokio_postgres::Client,
    desc: &Arc<RelDescriptor>,
    range: BlockRange,
) -> Vec<i64> {
    let (tx, mut rx) = mpsc::channel::<Vec<BackfillTuple>>(4);
    let stats = Arc::new(EmitterStats::default());
    let collect = tokio::spawn(async move {
        let mut out = Vec::new();
        while let Some(slab) = rx.recv().await {
            for t in slab {
                match t.columns.first().and_then(|c| c.as_ref()) {
                    Some(ColumnValue::Int8(v)) => out.push(*v),
                    other => panic!("unexpected id column {other:?}"),
                }
            }
        }
        out
    });
    copy_rows_into(client, desc, 42, &tx, &stats, range)
        .await
        .unwrap();
    drop(tx);
    let mut out = collect.await.unwrap();
    out.sort_unstable();
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chunked_copy_partitions_by_block_and_repeats_a_resumed_chunk() {
    if !fx::pg_available() {
        eprintln!("skip: no initdb on PATH");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let source = fx::start_source(&tmp);
    let _stop = fx::StopOnDrop { sh: &source };

    source
        .apply_schema_dump(&format!(
            "CREATE SCHEMA {SCHEMA};\n\
             CREATE TABLE {SCHEMA}.chunks (id bigint PRIMARY KEY, payload text NOT NULL);\n\
             INSERT INTO {SCHEMA}.chunks \
               SELECT g, {PAYLOAD} FROM generate_series(1, {N_ROWS}) g;\n"
        ))
        .unwrap();

    let blocks: i64 = source
        .psql_one(&format!(
            "SELECT pg_relation_size('{SCHEMA}.chunks') / current_setting('block_size')::int8"
        ))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(blocks > 8, "need a multi-block heap, got {blocks}");

    let plan = source
        .psql_one(&format!(
            "EXPLAIN (COSTS OFF) SELECT id FROM ONLY {SCHEMA}.chunks \
             WHERE ctid >= '(2,0)'::tid AND ctid < '(4,0)'::tid"
        ))
        .unwrap();
    assert!(
        plan.contains("Tid Range Scan"),
        "chunk predicate must plan as a TID range scan, got:\n{plan}"
    );

    let oid: u32 = source
        .psql_one(&format!("SELECT '{SCHEMA}.chunks'::regclass::oid::int8"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let desc = Arc::new(RelDescriptor {
        rfn: RelFileNode {
            spc_node: 0,
            db_node: 5,
            rel_node: oid,
        },
        oid,
        toast_oid: 0,
        namespace_oid: 0,
        rel_name: RelName::new(SCHEMA, "chunks"),
        kind: 'r',
        persistence: 'p',
        replident: ReplIdent::Nothing,
        attributes: vec![attr(1, "id", INT8OID), attr(2, "payload", TEXTOID)],
    });

    let client = open_sql_client(&fx::pg_cfg(&source, "copy-chunk-test"))
        .await
        .unwrap();

    let head = ids(
        &client,
        &desc,
        BlockRange {
            start: 0,
            end: Some(4),
        },
    )
    .await;
    let tail = ids(
        &client,
        &desc,
        BlockRange {
            start: 4,
            end: None,
        },
    )
    .await;
    assert!(
        !head.is_empty() && !tail.is_empty(),
        "both chunks carry rows"
    );
    assert_eq!(
        head.len() + tail.len(),
        N_ROWS as usize,
        "chunks partition the table"
    );
    assert!(
        head.last() < tail.first(),
        "block order is row order for an append-only load"
    );

    let repeat = ids(
        &client,
        &desc,
        BlockRange {
            start: 0,
            end: Some(4),
        },
    )
    .await;
    assert_eq!(
        repeat, head,
        "a resumed chunk re-reads exactly its own rows"
    );
}
