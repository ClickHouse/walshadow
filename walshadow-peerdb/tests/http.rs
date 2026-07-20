//! Acceptance drills from plans/future/peerdb.md against an in-process
//! mock control daemon speaking the TOML socket protocol

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::{Request, StatusCode};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use toml::{Table, Value as Toml};

use walshadow_peerdb::control::ControlClient;
use walshadow_peerdb::routes::{App, handle};
use walshadow_peerdb::state::Store;

/// Fixed source catalog: (namespace, relname, relreplident)
const CATALOG: &[(&str, &str, &str)] = &[
    ("public", "users", "d"),
    ("public", "orders", "f"),
    ("audit", "log", "d"),
];

/// Mock daemon mirroring the real one: `apply` merges into an accumulated
/// config table, `unset` masks it, and the read verbs answer from that
/// state so the handlers exercise real config-fragment logic
struct MockControl {
    cfg: Arc<Mutex<Table>>,
    rows_synced: Arc<Mutex<i64>>,
}

fn merge(base: &mut Table, over: Table) {
    for (k, v) in over {
        match (base.get_mut(&k), v) {
            (Some(Toml::Table(bt)), Toml::Table(ot)) => merge(bt, ot),
            (_, v) => {
                base.insert(k, v);
            }
        }
    }
}

fn mask(root: &mut Table, m: &Table) {
    for (k, v) in m {
        if let Toml::Table(sub) = v {
            if let Some(Toml::Table(t)) = root.get_mut(k) {
                mask(t, sub);
            }
        } else {
            root.remove(k);
        }
    }
}

fn selected_set(cfg: &Table) -> HashSet<(String, String)> {
    let mut out = HashSet::new();
    if let Some(Toml::Table(tbl)) = cfg.get("table") {
        for (ns, nsv) in tbl {
            if let Some(nst) = nsv.as_table() {
                for rel in nst.keys() {
                    out.insert((ns.clone(), rel.clone()));
                }
            }
        }
    }
    out
}

impl MockControl {
    fn spawn(dir: &std::path::Path) -> (PathBuf, Arc<Self>) {
        let socket = dir.join("control.sock");
        let mock = Arc::new(MockControl {
            cfg: Arc::new(Mutex::new(Table::new())),
            rows_synced: Arc::new(Mutex::new(0)),
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
                    // read to EOF: the client half-closes after writing
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 512];
                    loop {
                        match stream.read(&mut chunk).await {
                            Ok(0) => break,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                            Err(_) => return,
                        }
                    }
                    let text = String::from_utf8_lossy(&buf);
                    let (verb, body) = text.split_once('\n').unwrap_or((text.as_ref(), ""));
                    let body: Table = if body.trim().is_empty() {
                        Table::new()
                    } else {
                        body.parse().unwrap()
                    };
                    let resp = m.handle(verb.trim(), body);
                    let _ = stream.write_all(resp.as_bytes()).await;
                    let _ = stream.shutdown().await;
                });
            }
        });
        (socket, mock)
    }

    fn handle(&self, verb: &str, body: Table) -> String {
        match verb {
            "apply" => {
                merge(&mut self.cfg.lock().unwrap(), body);
                "OK\n".into()
            }
            "unset" => {
                mask(&mut self.cfg.lock().unwrap(), &body);
                "OK\n".into()
            }
            "reload" => "OK\n".into(),
            "status" => ok(&self.status_table()),
            "tables" => ok(&self.tables_table(body.get("namespace").and_then(Toml::as_str))),
            "schemas" => {
                let mut ns: Vec<&str> = CATALOG.iter().map(|(n, _, _)| *n).collect();
                ns.sort();
                ns.dedup();
                let mut out = Table::new();
                out.insert(
                    "schemas".into(),
                    Toml::Array(ns.into_iter().map(Into::into).collect()),
                );
                ok(&out)
            }
            "columns" => ok(&columns_table(body.get("relname").and_then(Toml::as_str))),
            other => format!("ERR unknown command {other}\n"),
        }
    }

    fn status_table(&self) -> Table {
        let cfg = self.cfg.lock().unwrap();
        let paused = cfg
            .get("stream")
            .and_then(Toml::as_table)
            .and_then(|t| t.get("paused"))
            .and_then(Toml::as_bool)
            .unwrap_or(false);
        let mut t = Table::new();
        t.insert("paused".into(), paused.into());
        t.insert(
            "rows_synced".into(),
            (*self.rows_synced.lock().unwrap()).into(),
        );
        t.insert("backfills_pending".into(), 0i64.into());
        t.insert("lag_bytes".into(), 2_097_152i64.into());
        t.insert("lag_seconds".into(), 0.0.into());
        t.insert("uptime_secs".into(), 0i64.into());
        t
    }

    fn tables_table(&self, ns_filter: Option<&str>) -> Table {
        let selected = selected_set(&self.cfg.lock().unwrap());
        let mut arr = Vec::new();
        for (ns, name, ri) in CATALOG {
            if ns_filter.is_some_and(|f| f != *ns) {
                continue;
            }
            let mut t = Table::new();
            t.insert(
                "selected".into(),
                selected
                    .contains(&(ns.to_string(), name.to_string()))
                    .into(),
            );
            t.insert("replica_identity".into(), (*ri).into());
            t.insert("namespace".into(), (*ns).into());
            t.insert("name".into(), (*name).into());
            arr.push(Toml::Table(t));
        }
        let mut out = Table::new();
        out.insert("tables".into(), Toml::Array(arr));
        out
    }

    fn cfg(&self) -> Table {
        self.cfg.lock().unwrap().clone()
    }
}

fn ok(body: &Table) -> String {
    format!("OK\n{}", toml::to_string(body).unwrap())
}

fn columns_table(relname: Option<&str>) -> Table {
    let cols: &[(&str, &str, bool)] = match relname {
        Some("users") => &[("id", "bigint", true), ("email", "text", false)],
        _ => &[],
    };
    let arr = cols
        .iter()
        .map(|(n, ty, nn)| {
            let mut t = Table::new();
            t.insert("name".into(), (*n).into());
            t.insert("type".into(), (*ty).into());
            t.insert("notnull".into(), (*nn).into());
            Toml::Table(t)
        })
        .collect();
    let mut out = Table::new();
    out.insert("columns".into(), Toml::Array(arr));
    out
}

/// Read a scalar at `cfg[section][key]` for assertions
fn at<'a>(cfg: &'a Table, section: &str, key: &str) -> Option<&'a Toml> {
    cfg.get(section)
        .and_then(Toml::as_table)
        .and_then(|t| t.get(key))
}

/// Whether the accumulated config opts `ns.rel` in
fn opted_in(cfg: &Table, ns: &str, rel: &str) -> bool {
    cfg.get("table")
        .and_then(Toml::as_table)
        .and_then(|t| t.get(ns))
        .and_then(Toml::as_table)
        .is_some_and(|t| t.contains_key(rel))
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
    let cfg = mock.cfg();
    assert_eq!(at(&cfg, "source", "host").unwrap().as_str(), Some("src-db"));
    assert_eq!(at(&cfg, "source", "port").unwrap().as_integer(), Some(5432));
    assert_eq!(at(&cfg, "source", "dbname").unwrap().as_str(), Some("app"));
    assert_eq!(
        at(&cfg, "source", "sslmode").unwrap().as_str(),
        Some("prefer")
    );
    assert_eq!(at(&cfg, "ch", "host").unwrap().as_str(), Some("ch"));
    assert_eq!(at(&cfg, "ch", "port").unwrap().as_integer(), Some(9000));
    assert_eq!(at(&cfg, "ch", "database").unwrap().as_str(), Some("cdc"));
    assert_eq!(at(&cfg, "ch", "secure").unwrap().as_bool(), Some(false));

    // validate both (structural under the TOML protocol)
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
    let cfg = mock.cfg();
    assert!(
        opted_in(&cfg, "public", "users") && opted_in(&cfg, "public", "orders"),
        "{cfg:?}"
    );
    assert_eq!(at(&cfg, "stream", "paused").unwrap().as_bool(), Some(false));

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

    // status: running, rows synced from the status reply
    *mock.rows_synced.lock().unwrap() = 1234;
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

    // pause → stream.paused = true, status PAUSED
    let (status, _) = call(
        &app,
        "POST",
        "/v1/mirrors/state_change",
        Some(json!({"flowJobName": "m1", "requestedFlowState": "STATUS_PAUSED"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        at(&mock.cfg(), "stream", "paused").unwrap().as_bool(),
        Some(true)
    );
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
    let cfg = mock.cfg();
    assert!(
        opted_in(&cfg, "public", "users")
            && opted_in(&cfg, "public", "orders")
            && opted_in(&cfg, "audit", "log"),
        "{cfg:?}"
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
    assert!(
        !mock.cfg().contains_key("table"),
        "terminate clears the opt-in set"
    );
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

    // schemas comes straight from the `schemas` verb
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

    // columns answers from the `columns` verb
    let (status, body) = call(
        &app,
        "GET",
        "/v1/peers/columns?peer_name=pg&schema_name=public&table_name=users",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let cols = body["columns"].as_array().unwrap();
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0]["name"], "id");
    assert_eq!(cols[0]["type"], "bigint");

    // slots synthesized from status; the daemon streams unless paused, so an
    // unpaused config presents the slot as active
    let (status, body) = call(&app, "GET", "/v1/peers/slots/pg", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["slotData"][0]["slotName"], "walshadow");
    assert_eq!(body["slotData"][0]["active"], true);
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

    // TOML bodies carry spaces: a spaced password applies verbatim
    let mut spaced = pg_peer("pg");
    spaced["peer"]["postgresConfig"]["password"] = json!("p w");
    spaced["allowUpdate"] = json!(true);
    let (status, body) = call(&app, "POST", "/v1/peers/create", Some(spaced)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], "CREATED");
    assert_eq!(
        at(&mock.cfg(), "source", "password").unwrap().as_str(),
        Some("p w")
    );

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
