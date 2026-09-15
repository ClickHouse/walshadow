//! A fast-path `ADD COLUMN jsonb DEFAULT '{}'` against a real ClickHouse.
//!
//! PG takes the fast path for a non-volatile default, so `attmissingval`
//! holds the *binary* jsonb datum — `00 00 00 20` for `{}` — and JSONB maps
//! to CH `JSON`. Rendering those bytes as a literal produced
//!
//!   `labels` JSON DEFAULT unhex('00000020')
//!
//! which ClickHouse rejects outright: exception 117, "Cannot parse JSON
//! object here ... default expression and column type are incompatible". CH
//! casts a column's DEFAULT to the column type at DDL time, so this is a
//! failed statement, not a wrong value — and since the DDL replays on every
//! boot, it crash-looped the daemon.
//!
//! Only the oracle can render a tier-3 datum, so the raw form carries no
//! DEFAULT at all. The text form, which the SQL path produces, still does.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::sync::Arc;

use walrus::pg::walparser::RelFileNode;

use walshadow::ch_ddl::{CreateShape, render_add_column, render_create_table};
use walshadow::column_rules::ColumnRulesBuilder;
use walshadow::mapping::{SystemColumns, TableTarget};
use walshadow::schema::{INT4OID, MissingDefault, RelAttr, RelDescriptor, RelName, ReplIdent};
use walshadow::type_bridge::map;

const JSONBOID: u32 = 3802;
/// `{}` as PG stores it: the jsonb container header, native-endian
const JSONB_EMPTY_OBJECT: [u8; 4] = [0x00, 0x00, 0x00, 0x20];

/// `attmissingval` is a one-element `anyarray`: header, then the element at
/// MAXALIGN(24), the jsonb datum behind its 4-byte varlena header
fn attmissingval_empty_object() -> Vec<u8> {
    let mut v = Vec::with_capacity(32);
    v.extend_from_slice(&0u32.to_le_bytes()); // vl_len_, ignored
    v.extend_from_slice(&1i32.to_le_bytes()); // ndim
    v.extend_from_slice(&0i32.to_le_bytes()); // dataoffset, no null bitmap
    v.extend_from_slice(&JSONBOID.to_le_bytes());
    v.extend_from_slice(&1i32.to_le_bytes()); // dims[0]
    v.extend_from_slice(&1i32.to_le_bytes()); // lbounds[0]
    let total = 4 + JSONB_EMPTY_OBJECT.len() as u32;
    v.extend_from_slice(&(total << 2).to_le_bytes());
    v.extend_from_slice(&JSONB_EMPTY_OBJECT);
    v
}

fn descriptor(name: &str, missing_default: Option<MissingDefault>) -> RelDescriptor {
    let mut id = jsonb_attr("execution_id", None);
    id.attnum = 1;
    id.name = "execution_id".into();
    id.type_oid = INT4OID;
    id.typmod = -1;
    id.type_name = "int4".into();
    id.type_byval = true;
    id.type_len = 4;
    id.type_storage = 'p';
    RelDescriptor {
        rfn: RelFileNode {
            spc_node: 1663,
            db_node: 5,
            rel_node: 16385,
        },
        oid: 16385,
        toast_oid: 0,
        namespace_oid: 2200,
        rel_name: RelName::new("public", name),
        kind: 'r',
        persistence: 'p',
        replident: ReplIdent::Default {
            pk_attnums: Some(vec![1]),
        },
        attributes: vec![id, jsonb_attr(name, missing_default)],
    }
}

fn jsonb_attr(name: &str, missing_default: Option<MissingDefault>) -> RelAttr {
    RelAttr {
        attnum: 2,
        name: name.into(),
        type_oid: JSONBOID,
        typmod: -1,
        not_null: true,
        dropped: false,
        type_name: "jsonb".into(),
        type_byval: false,
        type_len: -1,
        type_align: 'i',
        type_storage: 'x',
        missing_default,
    }
}

#[test]
fn clickhouse_accepts_the_ddl_a_jsonb_fast_path_default_renders() {
    if !fx::requirements_available() {
        return;
    }

    let slot = fx::Ports::alloc();
    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    ch.query(
        "CREATE OR REPLACE TABLE walshadow_test.recipe_executions (\
            execution_id UUID, _lsn UInt64, _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY execution_id",
    )
    .expect("create dest table");

    let cases: [(&str, Option<MissingDefault>); 3] = [
        (
            "labels",
            Some(MissingDefault::Raw(attmissingval_empty_object())),
        ),
        ("labels_text", Some(MissingDefault::Text("{}".into()))),
        ("labels_none", None),
    ];

    // CREATE is where CH validates a column's DEFAULT against its type; on
    // ALTER it lets the same bad default through
    for (name, missing_default) in cases.clone() {
        let sql = render_create_table(
            &descriptor(name, missing_default),
            &TableTarget::new("walshadow_test", name),
            &CreateShape {
                system: Arc::new(SystemColumns::default()),
                soft_delete: true,
                order_by: &[],
                primary_key: &[],
            },
            &ColumnRulesBuilder::new().finish().0,
        )
        .expect("renders")
        .expect("renderable");
        ch.query(&sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert!(
            !sql.contains("unhex("),
            "a JSON column cannot take a byte literal as its default: {sql}",
        );
        assert_eq!(
            ch.query(&format!(
                "SELECT type FROM system.columns WHERE database = 'walshadow_test' \
                 AND table = '{name}' AND name = '{name}'"
            ))
            .expect("ch system.columns"),
            "JSON",
        );
    }

    ch.query(
        "CREATE OR REPLACE TABLE walshadow_test.altered (\
            execution_id UUID, _lsn UInt64, _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY execution_id",
    )
    .expect("create alter target");
    for (name, missing_default) in cases {
        let resolved = map(&jsonb_attr(name, missing_default), false).expect("bridge maps jsonb");
        assert_eq!(resolved.ch_type, "JSON");
        let sql = render_add_column("walshadow_test.altered", name, &resolved);
        ch.query(&sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert!(
            !sql.contains("unhex("),
            "a JSON column cannot take a byte literal as its default: {sql}",
        );
    }

    // The text form keeps the default; the raw form has none to keep
    assert_eq!(
        ch.query(
            "SELECT groupArray(name) FROM system.columns \
             WHERE database = 'walshadow_test' AND table = 'altered' \
             AND name LIKE 'labels%' AND default_expression != ''"
        )
        .expect("ch defaults"),
        "['labels_text']",
    );
}
