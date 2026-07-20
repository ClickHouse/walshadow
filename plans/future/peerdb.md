# peerdb API shim (`walshadow-peerdb/`)

Second workspace crate exposing PeerDB's flow HTTP API — the grpc-gateway JSON
surface over `FlowService` (PeerDB `protos/route.proto`) — as a thin translator
onto the control daemon's TOML socket protocol (`ops/control.rs`). Goal: control
planes and UIs that already speak PeerDB (ClickPipes, peerdb-ui) drive walshadow
unchanged. The shim owns zero replication logic and none of walshadow's WAL /
native-protocol dependencies; process supervision, config persistence, streamer
launch stay in the control daemon. Dependency surface: tokio, serde, `toml`, an
HTTP server (bare hyper), unix-socket client

```
PeerDB client ──HTTP/JSON──▶ walshadow-peerdb ──unix socket──▶ walshadow-control ──▶ walshadow-stream
```

## Topology & cardinality

One shim ↔ one control socket ↔ one streamer. Mirror cardinality is exactly
one: `CreateCDCFlow` records `flow_job_name` and echoes it as `workflow_id`;
a second create without `attach_to_existing` returns `ALREADY_EXISTS`; with it,
returns the recorded id. `ListMirrors` / `ListMirrorNames` return a singleton
(or empty) list. Multi-pipe deployments run N containers, each with its own
control daemon + shim — no in-shim scheduling. Default bind `:8113`, matching
PeerDB's HTTP gateway port so existing client config carries over

## Wire fidelity

- **proto3-JSON per grpc-gateway**: lowerCamelCase fields, enums as strings
  (`"STATUS_RUNNING"`), 64-bit ints as strings, absent field = default value.
  Deserialization is tolerant: unknown fields ignored, missing fields
  defaulted — matches proto3 semantics, so PeerDB clients evolve without
  lockstep shim releases
- **Hand-written serde structs** for the consumed subset, not prost/pbjson
  codegen from vendored protos. Full codegen drags in hundreds of messages for
  a surface that is mostly stubs; the consumed subset is ~15 messages. Revisit
  if field drift becomes a recurring bug source
- **Errors** in grpc-gateway shape `{"code": <grpc code>, "message": …}` with
  the gateway's HTTP status mapping (3→400, 5→404, 6→409, 12→501, 13→500,
  14→503). `ERR <msg>` from the control socket maps to code 13 unless the
  handler knows better; socket connect failure maps to 14
- **Auth**: honor the `Authorization` header against a `PEERDB_PASSWORD`-style
  env var (constant-time compare), unauthenticated when unset — mirrors
  PeerDB gateway behavior

## Endpoint map

Four classes. *Mapped* endpoints drive the control socket; *served* endpoints
answer from shim/control state without side effects; *accept & ignore* return
success-shaped empty bodies so callers proceed; *reject* returns
`UNIMPLEMENTED`

### Mapped

| route | control action |
|---|---|
| `POST /v1/peers/create` | `postgres_config` → `apply` `[source]`; `clickhouse_config` → `apply` `[ch]`; peer name recorded in registry |
| `POST /v1/peers/validate` | structural check only — the protocol has no non-persisting connection probe (see Validation below) |
| `POST /v1/peers/drop` | forget registry entry; refuse while the mirror references it |
| `POST /v1/mirrors/cdc/validate` | `tables` — connecting to the applied source proves reachability + table existence in one call; no destination probe |
| `POST /v1/flows/cdc/create` | resolve `sourceName`/`destinationName` against registry, then one `apply` carrying `[table.<ns>.<rel>] replicate = true` per mapping plus `[stream] paused = false`; `workflow_id` = `flow_job_name` |
| `POST /v1/mirrors/state_change` | `STATUS_PAUSED` → `apply [stream] paused = true`; `STATUS_RUNNING` → `apply [stream] paused = false`; `STATUS_TERMINATED` → pause + `unset table` + forget mirror; `flowConfigUpdate.additionalTables`/`removedTables` → `apply` the opted-in blocks, `unset` the dropped ones |
| `POST /v1/mirrors/status` | `status` → `FlowStatus` (mapping below) + `CDCMirrorStatus` skeleton |
| `GET /v1/mirrors/list`, `/v1/mirrors/names` | singleton from mirror record + live `status` |

`TableMapping.sourceTableIdentifier` splits into (namespace, relname) at
ingress; dotted strings exist only at control-line interpolation.
`destinationTableIdentifier` differing from source naming is rejected until
per-table target rename exists in runtime config
([runtime_config_from_pg.md](runtime_config_from_pg.md))

### Served from state / introspection

| route | source |
|---|---|
| `GET /v1/peers/list`, `/info/{name}`, `/type/{name}` | registry; `peerdb_redacted` fields masked |
| `GET /v1/peers/schemas`, `/tables`, `/tables/all`, `/columns` | source-PG introspection via the `schemas` / `tables` (optional `namespace`) / `columns` (`namespace` + `relname`) verbs, which connect to the applied source |
| `GET /v1/peers/slots/{peer}`, `/stats/{peer}` | synthesized from `status` lag metrics; physical slot presented in logical-slot clothing, `active` = not paused |
| `GET /v1/mirrors/cdc/batches/*`, `cdc/graph`, `cdc/table_total_counts`, `total_rows_synced` | synthesized from metrics scrape (`emitter_rows` etc); one coarse synthetic batch per response, enough for UI rendering |
| `GET /v1/peers/columns/all_type_conversions` | static table of walshadow's PG→CH type map |
| `GET /v1/version`, `/v1/instance/info` | shim + streamer version, ready flag |

### Accept & ignore

`GET /v1/peers/publications` (empty list — walshadow consumes physical WAL,
publications don't exist in the model), alerts config CRUD, scripts CRUD,
dynamic settings, flow tags, maintenance + status + skip-snapshot-wait,
`sequences/reset`, `cancel_table_addition`, `slots/lag_history` (empty
series). Within accepted requests, ignored fields: `publicationName`,
`replicationSlotName`, `softDeleteColName`, `syncedAtColName`, snapshot
partition/parallelism knobs, `env`, `script`, `system`. Ignored non-empty
fields log at WARN once per key so silent divergence is greppable

### Reject (`UNIMPLEMENTED`)

`POST /v1/flows/qrep/create` (no qrep engine), `initialSnapshotOnly`,
`resync`. Faking success here would make callers believe a load ran

## FlowStatus mapping

| control `status` | FlowStatus |
|---|---|
| not paused, `backfills_pending > 0` | `STATUS_SNAPSHOT` |
| not paused | `STATUS_RUNNING` |
| `paused = true` | `STATUS_PAUSED` |
| socket unreachable | `STATUS_UNKNOWN` |
| mirror forgotten | `STATUS_TERMINATED` |

`paused` reflects the `[stream] paused` config flag, not streamer liveness; a
live daemon always answers running or paused, so UNKNOWN means the socket did
not answer (list/instance endpoints degrade to it, others surface 503).
`STATUS_PAUSING`/`STATUS_TERMINATING` transients unused — an `apply` reloads
synchronously

## Shim state

PeerDB persists peers/mirrors in a catalog PG; the shim persists a small JSON
state file (peer name → role + submitted config, mirror record: name,
table mappings, created-at). Connection-parameter truth stays in the control
daemon's state; the shim's copy exists to echo `GetPeerInfo` and to re-derive
`source`/`dest` role on peer reference. Single writer (the shim), same
durability model as the control daemon's `state.json`

## Control protocol

The daemon speaks a TOML socket protocol (`ops/control.rs`): one
`<verb>\n<toml body>` request per connection, EOF-framed, answered `OK\n[toml]`
or `ERR <msg>`. TOML bodies preserve scalar types and carry values with spaces
(passwords), so the shim needs no client-side quoting. Verbs it drives:

- `apply` / `unset` — merge a TOML fragment into `ch-config.d/50-api.toml`, or
  mask keys out of it; each validates the merged config and live-reloads, and
  only ever touches that one fragment so operator-owned base config stays
  read-only
- `status` — `paused`, `rows_synced`, `backfills_pending`, `lag_bytes`,
  `lag_seconds`, `uptime_secs`
- `tables` (optional `namespace`) / `schemas` / `columns` (`namespace` +
  `relname`) — source-PG introspection; `[[tables]]` carry `selected` and
  `replica_identity`, `[[columns]]` carry `name` / `type` / `notnull`

### Validation

The protocol has no non-persisting connection probe — `apply` mutates and
reloads. So `ValidatePeer` is structural (supported type + host present);
connectivity surfaces when `create_peer` applies the config, or on
`mirrors/cdc/validate`, which lists source tables over the live socket. There
is no destination probe: ClickHouse reachability first shows when the stream
runs. Per-column key membership isn't in the `columns` reply, so introspected
columns report `isKey`/`isReplicaIdentity` false

## Anti-goals

- **No Temporal semantics.** `workflow_id` is an echo of `flow_job_name`; no
  workflow history, retries, or signals
- **No publication / logical-slot management.** Physical WAL consumption;
  publication fields accepted and dropped
- **No qrep engine.** CDC only
- **No multi-mirror scheduling.** Cardinality one per daemon; N pipes = N
  deployments
- **No catalog metadata schema on source.** Nothing like `_peerdb_internal`;
  shim state is a local file
- **No soft-delete / synced-at column emulation.** Destination shape is
  walshadow's `_lsn` ReplacingMergeTree convergence model, not
  `_peerdb_is_deleted` / `_peerdb_synced_at`; readers of destination tables
  see walshadow's schema

## Open questions

- **gRPC listener.** Consumers wired to the flow API's gRPC port (8112)
  rather than the HTTP gateway get nothing from an HTTP-only shim. A tonic
  front sharing the handler layer is additive later; confirm what the target
  control plane actually speaks before building it
- **Destination-table lifecycle on terminate.** PeerDB drops destination
  tables unless `skipDestinationDrop`; control never drops them, so terminate
  behaves as `skipDestinationDrop = true` always. Visible to callers that
  recreate mirrors expecting a clean destination
- **Initial load fidelity.** `doInitialSnapshot` maps onto walshadow
  `initial_load` backfill; partitioning/parallelism knobs have no
  counterpart. `InitialLoadSummary` needs per-table backfill progress
  surfaced through control status before it can answer honestly
- **TableMapping column controls.** `exclude`, per-column settings, `engine`,
  `partitionByExpr` map naturally onto runtime-config column/table overrides —
  several of which are themselves future work
  ([runtime_config_from_pg.md](runtime_config_from_pg.md)). Until then:
  accept-and-ignore with WARN, or reject non-empty? Rejection is honest but
  may block UI-driven creates that always send defaults
- **Peer names vs stored config drift.** Registry keeps the submitted peer
  config; control keeps the applied one. An operator editing via the control
  CLI directly leaves the shim's echo stale. Option: `GetPeerInfo` re-reads
  the daemon's `show` config and merges, treating control as truth for
  connection fields
- **Type-conversion endpoint fidelity.** UI column-type pickers read
  `all_type_conversions`; serving walshadow's real map constrains what the UI
  offers. Serving empty disables pickers — probably the safer start

## Acceptance drills

- **curl lifecycle.** Create PG peer, create CH peer, validate both, create
  CDC mirror over two tables → `status` not paused, `MirrorStatus` =
  `STATUS_RUNNING`. Insert rows on source → `total_rows_synced` climbs.
  `state_change` PAUSED pauses the streamer; RUNNING resumes;
  `flowConfigUpdate.additionalTables` grows the opt-in set and backfills;
  TERMINATED stops + clears, `mirrors/list` empties
- **peerdb-ui smoke.** UI pointed at the shim renders peer list, mirror
  overview, and status page without errors — batches/graph endpoints return
  well-formed empties, redacted peer info displays
- **Ignore surface.** `POST /v1/alerts/config` returns success shape;
  `GET /v1/peers/publications` returns empty list; qrep create returns 501
  with grpc-shaped body; create request carrying `softDeleteColName` succeeds
  and logs one WARN
- **Tolerant decode.** Create requests from a PeerDB release newer than the
  shim (extra unknown fields) parse and apply; absent optional fields behave
  as proto3 defaults
