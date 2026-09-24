#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::sync::Arc;

use walshadow::backfill_staging::{StagingSession, prepare};
use walshadow::backfill_types::BackupRequest;
use walshadow::backup_checkpoint::BackupCheckpoint;
use walshadow::ch_emitter::EmitterConfig;
use walshadow::mapping::{TableMapping, TableTarget, mapping_handle};
use walshadow::runtime_config::InitialLoadMode;
use walshadow::schema::{RelDescriptor, RelName, ReplIdent};

#[tokio::test]
async fn checkpoint_reuses_staging_and_rejects_replaced_table() {
    let ports = fx::Ports::alloc();
    let ch =
        fx::ChServer::spawn(tempfile::tempdir().unwrap(), ports.ch_tcp, ports.ch_http).unwrap();
    ch.query("CREATE TABLE default.t (id UInt64, value String, _lsn UInt64) ENGINE = ReplacingMergeTree(_lsn) ORDER BY id").unwrap();
    let dest = walshadow::config::DestEmitter::new(
        Arc::new(EmitterConfig {
            port: ports.ch_tcp,
            ..Default::default()
        }),
        None,
    );
    let emitter = dest.current();
    let name = RelName::new("public", "t");
    let mapping = mapping_handle(
        [(
            name.clone(),
            TableMapping {
                target: TableTarget::new("default", "t"),
                columns: Vec::new(),
            },
        )]
        .into_iter()
        .collect(),
    );
    let mut requests = vec![BackupRequest {
        s_lsn: 10,
        desc: Arc::new(RelDescriptor {
            rfn: walrus::pg::walparser::RelFileNode {
                spc_node: 0,
                db_node: 5,
                rel_node: 16400,
            },
            oid: 16400,
            toast_oid: 0,
            namespace_oid: 2200,
            rel_name: name,
            kind: 'r',
            persistence: 'p',
            replident: ReplIdent::Nothing,
            attributes: Vec::new(),
        }),
    }];
    let plan = prepare(dest.clone(), &mapping, &requests, false)
        .await
        .unwrap();
    let snapshot = mapping.snapshot().await;
    let mut checkpoint = BackupCheckpoint::new(
        InitialLoadMode::ObjectStore,
        &requests,
        &snapshot,
        &emitter,
        None,
    );
    let mut session = StagingSession::connect(dest.clone()).await.unwrap();
    checkpoint
        .capture_staging(&plan, &mut session)
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    checkpoint.spool.bytes = 100;
    checkpoint.offset = 40;
    checkpoint.save(dir.path()).await.unwrap();
    ch.query("INSERT INTO default.t__wsstg VALUES (1, 'kept', 10)")
        .unwrap();

    let resumed = BackupCheckpoint::load(dir.path()).await.unwrap().unwrap();
    assert!(resumed.matches(&checkpoint));
    assert!(resumed.staging_intact(&mut session).await.unwrap());
    prepare(dest.clone(), &mapping, &requests, true)
        .await
        .unwrap();
    assert!(resumed.staging_intact(&mut session).await.unwrap());
    ch.query("INSERT INTO default.t__wsstg VALUES (1, 'kept', 10)")
        .unwrap();
    assert_eq!(
        ch.query("SELECT count() FROM default.t__wsstg FINAL")
            .unwrap(),
        "1"
    );
    assert_eq!(
        ch.query("SELECT value FROM default.t__wsstg FINAL")
            .unwrap(),
        "kept"
    );

    requests[0].s_lsn += 1;
    assert!(!resumed.matches(&BackupCheckpoint::new(
        InitialLoadMode::ObjectStore,
        &requests,
        &snapshot,
        &emitter,
        None
    )));
    prepare(dest, &mapping, &requests, false).await.unwrap();
    assert!(!resumed.staging_intact(&mut session).await.unwrap());
}
