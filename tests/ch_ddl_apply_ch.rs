//! DDL applicator against a real ClickHouse: mapped re-create adds
//! missing columns, a key retype surfaces as a config error naming the
//! remedy, strategy=drop drops the destination and forgets the mapping,
//! and a failing SELECT surfaces the server exception.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::sync::Arc;
use std::time::Duration;

use foldhash::HashMap;
use tokio::sync::watch;
use walrus::pg::walparser::RelFileNode;
use walshadow::ch::{CompressionChoice, EmitterError, connect_client, query_strings};
use walshadow::ch_ddl::{DdlApplicator, DdlBoot, DdlConfig, Stamp};
use walshadow::ch_emitter::{EmitterConfig, InstanceConfig, RetryConfig};
use walshadow::config::{DestEmitter, ResolvedConfig};
use walshadow::mapping::{ColumnMapping, DropTableStrategy, TableMapping, TableTarget};
use walshadow::schema::{
    INT4OID, INT8OID, RelAttr, RelDescriptor, RelName, ReplIdent, SchemaDiff, SchemaEvent, TEXTOID,
};

const DB: &str = "ddl_apply";

fn attr(attnum: i16, name: &str, type_oid: u32, not_null: bool) -> RelAttr {
    RelAttr {
        attnum,
        name: name.into(),
        type_oid,
        typmod: -1,
        not_null,
        dropped: false,
        type_name: "test".into(),
        type_byval: type_oid != TEXTOID,
        type_len: if type_oid == TEXTOID { -1 } else { 4 },
        type_align: 'i',
        type_storage: if type_oid == TEXTOID { 'x' } else { 'p' },
        missing_default: None,
    }
}

fn rel(oid: u32, name: &str, attributes: Vec<RelAttr>) -> RelDescriptor {
    RelDescriptor {
        rfn: RelFileNode {
            spc_node: 1663,
            db_node: 5,
            rel_node: oid,
        },
        oid,
        toast_oid: 0,
        namespace_oid: 2200,
        rel_name: RelName::new("public", name),
        kind: 'r',
        persistence: 'p',
        ancestors: Vec::new(),
        replident: ReplIdent::Default {
            pk_attnums: Some(vec![1]),
        },
        attributes,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ddl_applies_and_refuses_against_clickhouse() {
    if !fx::tools::clickhouse_available() {
        return;
    }
    let slot = fx::Ports::alloc();
    let ch = fx::ChServer::spawn(tempfile::tempdir().unwrap(), slot.ch_tcp, slot.ch_http)
        .expect("spawn ch");
    ch.query(&format!("CREATE DATABASE {DB}"))
        .expect("create db");
    let emitter = EmitterConfig {
        conn: InstanceConfig {
            host: "127.0.0.1".into(),
            port: slot.ch_tcp,
            database: DB.into(),
            compression: CompressionChoice::None,
            ..Default::default()
        },
        // Refusals retry like any server exception, keep them quick
        retry: RetryConfig {
            max_attempts: 1,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
        },
        ..Default::default()
    };
    let resolved = Arc::new(ResolvedConfig::default());
    let boot = DdlBoot {
        target_database: DB.into(),
        source_database: "app".into(),
        soft_delete: false,
        system: Arc::default(),
        replicate_all: true,
        auto_create_name_all: None,
        runtime_config_schema: None,
    };
    let cfg = DdlConfig::from_resolved(&resolved, boot).with_drop_strategy(DropTableStrategy::Drop);
    let pinned = RelName::new("public", "pinned");
    let pinned_mapping = TableMapping {
        target: TableTarget::new(DB, "pinned"),
        columns: vec![ColumnMapping {
            src_attnum: 1,
            target_name: "id".into(),
            target_type: "Int32".into(),
            type_pinned: false,
        }],
        tees: Vec::new(),
    };
    let mapping =
        walshadow::mapping::mapping_handle(HashMap::from_iter([(pinned.clone(), pinned_mapping)]));
    let dest = DestEmitter::new(Arc::new(emitter.clone()), None);
    let mut app = DdlApplicator::new(dest, cfg, mapping.clone(), watch::channel(resolved).1)
        .await
        .expect("applicator connects");
    let at = Stamp::default();

    // Mapped relation: create from the mapping, then add its columns
    let pinned_desc = rel(16390, "pinned", vec![attr(1, "id", INT4OID, true)]);
    let added = SchemaEvent::Added {
        desc: Arc::new(pinned_desc),
    };
    app.apply_at(&added, None, at).await.unwrap();
    assert_eq!(
        (app.stats.creates_applied, app.stats.alters_applied),
        (1, 1)
    );

    // Zero-column relation: re-learn creates from its mapping with nothing to add
    let bare = SchemaEvent::Added {
        desc: Arc::new(rel(16395, "bare", Vec::new())),
    };
    app.apply_at(&bare, None, at).await.unwrap();
    app.apply_at(&bare, None, at).await.unwrap();
    assert_eq!(
        (app.stats.creates_applied, app.stats.alters_applied),
        (3, 1)
    );

    // Refusal other than a key retype passes through unchanged
    let gone_v1 = rel(
        16396,
        "gone",
        vec![attr(1, "id", INT4OID, true), attr(2, "n", INT4OID, false)],
    );
    app.apply_at(
        &SchemaEvent::Added {
            desc: Arc::new(gone_v1.clone()),
        },
        None,
        at,
    )
    .await
    .unwrap();
    let gone = mapping
        .snapshot()
        .get(&gone_v1.rel_name)
        .map(|m| m.target.clone())
        .expect("derived mapping");
    ch.query(&format!("DROP TABLE {}", gone.sql()))
        .expect("drop behind applicator");
    let mut gone_v2 = gone_v1.clone();
    gone_v2.attributes[1].type_oid = INT8OID;
    gone_v2.attributes[1].type_len = 8;
    let diff = SchemaDiff {
        type_changes: vec![(gone_v1.attributes[1].clone(), gone_v2.attributes[1].clone())],
        ..SchemaDiff::default()
    };
    let retyped = SchemaEvent::Changed {
        old: Arc::new(gone_v1),
        new: Arc::new(gone_v2),
        diff,
    };
    let e = app.apply_at(&retyped, None, at).await.unwrap_err();
    assert!(
        matches!(&e, EmitterError::ServerException { code: 60, .. }),
        "{e}"
    );

    // Replicate-all derives the destination for a fresh relation
    let v1 = rel(
        16400,
        "t",
        vec![
            attr(1, "id", INT4OID, true),
            attr(2, "name", TEXTOID, false),
        ],
    );
    let added = SchemaEvent::Added {
        desc: Arc::new(v1.clone()),
    };
    app.apply_at(&added, None, at).await.unwrap();
    let target = mapping
        .snapshot()
        .get(&v1.rel_name)
        .map(|m| m.target.clone())
        .expect("derived mapping");

    // Sorting-key retype: ClickHouse refuses, error names the remedy
    let mut v2 = v1.clone();
    v2.attributes[0].type_oid = INT8OID;
    v2.attributes[0].type_len = 8;
    let diff = SchemaDiff {
        type_changes: vec![(v1.attributes[0].clone(), v2.attributes[0].clone())],
        ..SchemaDiff::default()
    };
    let retyped = SchemaEvent::Changed {
        old: Arc::new(v1.clone()),
        new: Arc::new(v2),
        diff,
    };
    let e = app.apply_at(&retyped, None, at).await.unwrap_err();
    assert!(
        matches!(&e, EmitterError::Config(m) if m.contains("rebuild destination manually")),
        "{e}"
    );

    // strategy=drop removes the destination and forgets the mapping
    let dropped = SchemaEvent::Dropped {
        oid: v1.oid,
        rel_name: v1.rel_name.clone(),
        partition: None,
    };
    app.apply_at(&dropped, None, at).await.unwrap();
    assert_eq!(app.stats.drops_applied, 1);
    assert!(mapping.snapshot().get(&v1.rel_name).is_none());
    let exists = ch
        .query(&format!("EXISTS TABLE {}", target.sql()))
        .expect("exists query");
    assert_eq!(exists.trim(), "0");

    let mut client = connect_client(&emitter).await.expect("connect");
    let e = query_strings(&mut client, "SELECT nope FROM nowhere")
        .await
        .unwrap_err();
    assert!(matches!(e, EmitterError::ServerException { .. }), "{e}");
}
