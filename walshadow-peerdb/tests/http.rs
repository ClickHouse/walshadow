//! Acceptance drills from plans/future/peerdb.md against an in-process
//! mock control daemon speaking the line protocol

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::{Request, StatusCode};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use walshadow_peerdb::control::ControlClient;
use walshadow_peerdb::routes::{App, handle};
use walshadow_peerdb::state::Store;

/// Scripted control daemon: records request lines, answers from a handler
struct MockControl {
    lines: Arc<Mutex<Vec<String>>>,
    /// `stream status` payload toggled by stream start/stop
    running: Arc<Mutex<bool>>,
    /// extra kv appended to a running `stream status` payload
    status_extra: Arc<Mutex<String>>,
}

impl MockControl {
    fn spawn(dir: &std::path::Path) -> (PathBuf, Arc<Self>) {
        let socket = dir.join("control.sock");
        let mock = Arc::new(MockControl {
            lines: Arc::new(Mutex::new(Vec::new())),
            running: Arc::new(Mutex::new(false)),
            status_extra: Arc::new(Mutex::new(String::new())),
        });
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        listener.set_nonblocking(true).unwrap();
        let listener = tokio::net::UnixListener::from_std(listener).unwrap();
        let m = mock.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let m = m.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 512];
                    loop {
                        let Ok(n) = stream.read(&mut chunk).await else {
                            return;
                        };
                        if n == 0 {
                            break;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                        if buf.contains(&b'\n') {
                            break;
                        }
                    }
                    let line = String::from_utf8_lossy(&buf);
                    let line = line.split('\n').next().unwrap_or("").trim().to_string();
                    let resp = m.handle(&line);
                    m.lines.lock().unwrap().push(line);
                    let _ = stream.write_all(resp.as_bytes()).await;
                    let _ = stream.shutdown().await;
                });
            }
        });
        (socket, mock)
    }

    fn handle(&self, line: &str) -> String {
        let verb = line
            .split_whitespace()
            .take(2)
            .collect::<Vec<_>>()
            .join(" ");
        match verb.as_str() {
            "source set" | "dest set" | "source test" | "dest test" | "tables select"
            | "tables clear" => "OK\n".into(),
            "stream start" => {
                *self.running.lock().unwrap() = true;
                "OK\npid=42\n".into()
            }
            "stream stop" => {
                *self.running.lock().unwrap() = false;
                "OK\n".into()
            }
            "stream status" => {
                if *self.running.lock().unwrap() {
                    let extra = self.status_extra.lock().unwrap().clone();
                    format!("OK\nstate=running\npid=42\nlag_bytes=2097152\n{extra}")
                } else {
                    "OK\nstate=stopped\n".into()
                }
            }
            "tables list" => {
                "OK\npublic.users\tno\tdefault\npublic.orders\tno\tfull\naudit.log\tno\tdefault\n"
                    .into()
            }
            _ => format!("ERR unknown command {verb}\n"),
        }
    }

    fn lines(&self) -> Vec<String> {
        self.lines.lock().unwrap().clone()
    }
}

async fn shim(dir: &std::path::Path, password: Option<&str>) -> (App, Arc<MockControl>) {
    let (socket, mock) = MockControl::spawn(dir);
    let app = App {
        control: ControlClient::new(socket),
        store: Store::load(dir.join("state.json")).await.unwrap(),
        password: password.map(str::to_string),
        version: "walshadow-peerdb-test".into(),
    };
    (app, mock)
}

async fn call(app: &App, method: &str, path: &str, body: Option<Value>) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json")
        .body(match &body {
            Some(v) => Full::new(Bytes::from(v.to_string())),
            None => Full::default(),
        })
        .unwrap();
    let resp = handle(app, req).await;
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, value)
}

fn pg_peer(name: &str) -> Value {
    json!({"peer": {
        "name": name, "type": "POSTGRES",
        "postgresConfig": {
            "host": "src-db", "port": 5432, "user": "postgres",
            "password": "pgpw", "database": "app"
        }
    }})
}

fn ch_peer(name: &str) -> Value {
    json!({"peer": {
        "name": name, "type": "CLICKHOUSE",
        "clickhouseConfig": {
            "host": "ch", "port": 9000, "user": "default",
            "password": "chpw", "database": "cdc", "disableTls": true
        }
    }})
}

fn cdc_create(flow: &str, tables: &[&str]) -> Value {
    json!({"connectionConfigs": {
        "flowJobName": flow,
        "sourceName": "pg", "destinationName": "ch",
        "doInitialSnapshot": true,
        "tableMappings": tables
            .iter()
            .map(|t| json!({"sourceTableIdentifier": t}))
            .collect::<Vec<_>>(),
    }})
}

#[tokio::test]
async fn curl_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let (app, mock) = shim(dir.path(), None).await;

    // create both peers
    let (status, body) = call(&app, "POST", "/v1/peers/create", Some(pg_peer("pg"))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], "CREATED");
    let (status, body) = call(&app, "POST", "/v1/peers/create", Some(ch_peer("ch"))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], "CREATED");
    let lines = mock.lines();
    assert!(
        lines
            .iter()
            .any(|l| l == "source set host=src-db port=5432 dbname=app user=postgres password=pgpw sslmode=prefer"),
        "{lines:?}"
    );
    assert!(
        lines.iter().any(|l| l
            == "dest set host=ch port=9000 database=cdc user=default password=chpw secure=false"),
        "{lines:?}"
    );

    // validate both (kv overrides ride on `test`)
    let (status, body) = call(&app, "POST", "/v1/peers/validate", Some(pg_peer("pg"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "VALID");
    let (_, body) = call(&app, "POST", "/v1/peers/validate", Some(ch_peer("ch"))).await;
    assert_eq!(body["status"], "VALID");

    // validate then create the mirror over two tables
    let create = cdc_create("m1", &["public.users", "public.orders"]);
    let (status, body) = call(
        &app,
        "POST",
        "/v1/mirrors/cdc/validate",
        Some(create.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = call(&app, "POST", "/v1/flows/cdc/create", Some(create.clone())).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["workflowId"], "m1");
    let lines = mock.lines();
    assert!(
        lines
            .iter()
            .any(|l| l == "tables select public.users public.orders"),
        "{lines:?}"
    );
    assert!(lines.iter().any(|l| l == "stream start"), "{lines:?}");

    // duplicate create without attach → ALREADY_EXISTS
    let (status, body) = call(&app, "POST", "/v1/flows/cdc/create", Some(create.clone())).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["code"], 6);
    // with attach → recorded workflow id
    let mut attach = create.clone();
    attach["attachToExisting"] = json!(true);
    let (status, body) = call(&app, "POST", "/v1/flows/cdc/create", Some(attach)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["workflowId"], "m1");

    // status: running, rows synced from control extension key
    *mock.status_extra.lock().unwrap() = "rows_synced=1234\n".into();
    let (status, body) = call(
        &app,
        "POST",
        "/v1/mirrors/status",
        Some(json!({"flowJobName": "m1"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["currentFlowState"], "STATUS_RUNNING");
    assert_eq!(body["cdcStatus"]["rowsSynced"], "1234");
    assert_eq!(body["cdcStatus"]["config"]["flowJobName"], "m1");
    assert_eq!(body["cdcStatus"]["cdcBatches"][0]["numRows"], "1234");

    // rows counter endpoints
    let (_, body) = call(&app, "GET", "/v1/mirrors/total_rows_synced/m1", None).await;
    assert_eq!(body["totalCount"], "1234");

    // pause → stream stop, status PAUSED
    let (status, _) = call(
        &app,
        "POST",
        "/v1/mirrors/state_change",
        Some(json!({"flowJobName": "m1", "requestedFlowState": "STATUS_PAUSED"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(mock.lines().iter().any(|l| l == "stream stop"));
    let (_, body) = call(
        &app,
        "POST",
        "/v1/mirrors/status",
        Some(json!({"flowJobName": "m1"})),
    )
    .await;
    assert_eq!(body["currentFlowState"], "STATUS_PAUSED");

    // resume
    let (status, _) = call(
        &app,
        "POST",
        "/v1/mirrors/state_change",
        Some(json!({"flowJobName": "m1", "requestedFlowState": "STATUS_RUNNING"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // additionalTables grows the opt-in set
    let (status, body) = call(
        &app,
        "POST",
        "/v1/mirrors/state_change",
        Some(json!({
            "flowJobName": "m1",
            "requestedFlowState": "STATUS_UNKNOWN",
            "flowConfigUpdate": {"cdcFlowConfigUpdate": {
                "additionalTables": [{"sourceTableIdentifier": "audit.log"}]
            }},
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        mock.lines()
            .iter()
            .any(|l| l == "tables select public.users public.orders audit.log"),
        "{:?}",
        mock.lines()
    );

    // mirror listing shows the singleton
    let (_, body) = call(&app, "GET", "/v1/mirrors/list", None).await;
    assert_eq!(body["mirrors"][0]["name"], "m1");
    assert_eq!(body["mirrors"][0]["status"], "STATUS_RUNNING");
    let (_, body) = call(&app, "GET", "/v1/mirrors/names", None).await;
    assert_eq!(body["names"], json!(["m1"]));

    // terminate stops, clears, forgets; list empties, status answers TERMINATED
    let (status, _) = call(
        &app,
        "POST",
        "/v1/mirrors/state_change",
        Some(json!({"flowJobName": "m1", "requestedFlowState": "STATUS_TERMINATED"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(mock.lines().iter().any(|l| l == "tables clear"));
    let (_, body) = call(&app, "GET", "/v1/mirrors/list", None).await;
    assert_eq!(body["mirrors"], json!([]));
    let (status, body) = call(
        &app,
        "POST",
        "/v1/mirrors/status",
        Some(json!({"flowJobName": "m1"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["currentFlowState"], "STATUS_TERMINATED");
}

#[tokio::test]
async fn peer_registry_rules() {
    let dir = tempfile::tempdir().unwrap();
    let (app, _mock) = shim(dir.path(), None).await;

    let (status, _) = call(&app, "POST", "/v1/peers/create", Some(pg_peer("pg"))).await;
    assert_eq!(status, StatusCode::OK);
    // same name again without allowUpdate → 409
    let (status, body) = call(&app, "POST", "/v1/peers/create", Some(pg_peer("pg"))).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["code"], 6);
    // same name with allowUpdate → ok
    let mut update = pg_peer("pg");
    update["allowUpdate"] = json!(true);
    let (status, _) = call(&app, "POST", "/v1/peers/create", Some(update)).await;
    assert_eq!(status, StatusCode::OK);
    // second postgres peer under a different name → FAILED, slot held
    let (status, body) = call(&app, "POST", "/v1/peers/create", Some(pg_peer("pg2"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "FAILED");

    // info is redacted, type echoes
    let (_, body) = call(&app, "GET", "/v1/peers/info/pg", None).await;
    assert_eq!(body["peer"]["postgresConfig"]["password"], "********");
    assert_eq!(body["peer"]["postgresConfig"]["host"], "src-db");
    let (_, body) = call(&app, "GET", "/v1/peers/type/pg", None).await;
    assert_eq!(body["peerType"], "POSTGRES");
    let (status, body) = call(&app, "GET", "/v1/peers/info/nope", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], 5);

    // list buckets by role
    let (_, body) = call(&app, "POST", "/v1/peers/create", Some(ch_peer("ch"))).await;
    assert_eq!(body["status"], "CREATED");
    let (_, body) = call(&app, "GET", "/v1/peers/list", None).await;
    assert_eq!(body["items"].as_array().unwrap().len(), 2);
    assert_eq!(body["sourceItems"][0]["name"], "pg");
    assert_eq!(body["destinationItems"][0]["name"], "ch");

    // drop refused while mirror references the peer
    let (status, _) = call(
        &app,
        "POST",
        "/v1/flows/cdc/create",
        Some(cdc_create("m1", &["public.users"])),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = call(
        &app,
        "POST",
        "/v1/peers/drop",
        Some(json!({"peerName": "pg"})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], 9);
}

#[tokio::test]
async fn introspection_endpoints() {
    let dir = tempfile::tempdir().unwrap();
    let (app, _mock) = shim(dir.path(), None).await;
    call(&app, "POST", "/v1/peers/create", Some(pg_peer("pg"))).await;

    // schemas fall back to namespaces from `tables list` pre-extension
    let (status, body) = call(&app, "GET", "/v1/peers/schemas?peer_name=pg", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["schemas"], json!(["audit", "public"]));

    let (_, body) = call(
        &app,
        "GET",
        "/v1/peers/tables?peerName=pg&schemaName=public",
        None,
    )
    .await;
    let tables = body["tables"].as_array().unwrap();
    assert_eq!(tables.len(), 2);
    assert_eq!(tables[0]["tableName"], "users");
    assert_eq!(tables[1]["isReplicaIdentityFull"], true);

    let (_, body) = call(&app, "GET", "/v1/peers/tables/all?peer_name=pg", None).await;
    assert_eq!(
        body["tables"],
        json!(["public.users", "public.orders", "audit.log"])
    );

    // columns needs the control extension → 501 until it lands
    let (status, body) = call(
        &app,
        "GET",
        "/v1/peers/columns?peer_name=pg&schema_name=public&table_name=users",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{body}");

    // slots synthesized from stream status
    let (status, body) = call(&app, "GET", "/v1/peers/slots/pg", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["slotData"][0]["slotName"], "walshadow");
    assert_eq!(body["slotData"][0]["active"], false);
    let (_, body) = call(&app, "GET", "/v1/peers/stats/pg", None).await;
    assert_eq!(body["statData"], json!([]));
}

#[tokio::test]
async fn ignore_and_reject_surface() {
    let dir = tempfile::tempdir().unwrap();
    let (app, _mock) = shim(dir.path(), None).await;
    call(&app, "POST", "/v1/peers/create", Some(pg_peer("pg"))).await;
    call(&app, "POST", "/v1/peers/create", Some(ch_peer("ch"))).await;

    // alerts config accepts with a success shape
    let (status, _) = call(
        &app,
        "POST",
        "/v1/alerts/config",
        Some(json!({"config": {"serviceType": "slack"}})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, body) = call(&app, "GET", "/v1/alerts/config", None).await;
    assert_eq!(body["configs"], json!([]));

    // publications are empty by model
    let (_, body) = call(&app, "GET", "/v1/peers/publications?peer_name=pg", None).await;
    assert_eq!(body["publicationNames"], json!([]));

    // qrep create → 501 with grpc-shaped body
    let (status, body) = call(
        &app,
        "POST",
        "/v1/flows/qrep/create",
        Some(json!({"qrepConfig": {"flowJobName": "q"}})),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(body["code"], 12);
    assert!(body["message"].as_str().unwrap().contains("qrep"));

    // create carrying ignored fields succeeds (WARN once, not observable here)
    let mut create = cdc_create("m1", &["public.users"]);
    create["connectionConfigs"]["softDeleteColName"] = json!("_peerdb_is_deleted");
    create["connectionConfigs"]["publicationName"] = json!("pub");
    let (status, body) = call(&app, "POST", "/v1/flows/cdc/create", Some(create)).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // initialSnapshotOnly / resync are honest rejections
    let mut snap = cdc_create("m2", &["public.users"]);
    snap["connectionConfigs"]["initialSnapshotOnly"] = json!(true);
    let (status, _) = call(&app, "POST", "/v1/mirrors/cdc/validate", Some(snap)).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);

    // per-table rename rejected until runtime-config rename exists
    let mut renamed = cdc_create("m3", &["public.users"]);
    renamed["connectionConfigs"]["tableMappings"][0]["destinationTableIdentifier"] =
        json!("renamed");
    let (status, _) = call(&app, "POST", "/v1/mirrors/cdc/validate", Some(renamed)).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);

    // unknown /v1 path → 501 grpc shape
    let (status, body) = call(&app, "GET", "/v1/flows/unheard_of", None).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(body["code"], 12);

    // instance/version render
    let (_, body) = call(&app, "GET", "/v1/version", None).await;
    assert_eq!(body["version"], "walshadow-peerdb-test");
    let (_, body) = call(&app, "GET", "/v1/instance/info", None).await;
    assert_eq!(body["status"], "INSTANCE_STATUS_READY");
}

#[tokio::test]
async fn tolerant_decode_and_errors() {
    let dir = tempfile::tempdir().unwrap();
    let (app, mock) = shim(dir.path(), None).await;
    call(&app, "POST", "/v1/peers/create", Some(pg_peer("pg"))).await;
    call(&app, "POST", "/v1/peers/create", Some(ch_peer("ch"))).await;

    // fields from a newer PeerDB release parse and apply
    let mut create = cdc_create("m1", &["public.users"]);
    create["connectionConfigs"]["fieldFromTheFuture"] = json!({"nested": [1, 2]});
    create["unknownTopLevel"] = json!("x");
    let (status, body) = call(&app, "POST", "/v1/flows/cdc/create", Some(create)).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // password with a space rejected until control gains value quoting
    let mut spaced = pg_peer("pg2");
    spaced["peer"]["postgresConfig"]["password"] = json!("p w");
    let (status, body) = call(&app, "POST", "/v1/peers/validate", Some(spaced)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], 3);

    // malformed body → grpc-shaped 400
    let req = Request::builder()
        .method("POST")
        .uri("/v1/mirrors/status")
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from("{not json")))
        .unwrap();
    let resp = handle(&app, req).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["code"], 3);

    // unknown mirror → 404 grpc shape
    let (status, body) = call(
        &app,
        "POST",
        "/v1/mirrors/status",
        Some(json!({"flowJobName": "ghost"})),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], 5);

    // control daemon down → 503
    drop(mock);
    let dir2 = tempfile::tempdir().unwrap();
    let downed = App {
        control: ControlClient::new(dir2.path().join("missing.sock")),
        store: Store::load(dir2.path().join("state.json")).await.unwrap(),
        password: None,
        version: "t".into(),
    };
    let (status, body) = call(&downed, "POST", "/v1/peers/create", Some(pg_peer("pg"))).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["code"], 14);
}

#[tokio::test]
async fn auth_gate() {
    let dir = tempfile::tempdir().unwrap();
    let (app, _mock) = shim(dir.path(), Some("s3cret")).await;

    let (status, body) = call(&app, "GET", "/v1/version", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["code"], 16);

    let req = Request::builder()
        .method("GET")
        .uri("/v1/version")
        .header("authorization", "Bearer s3cret")
        .body(Full::<Bytes>::default())
        .unwrap();
    let resp = handle(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let req = Request::builder()
        .method("GET")
        .uri("/v1/version")
        .header("authorization", "Bearer wrong")
        .body(Full::<Bytes>::default())
        .unwrap();
    let resp = handle(&app, req).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}
