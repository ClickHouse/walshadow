# robot:shadow

## sources of truth

- plans/shadow.md
- src/shadow_catalog.rs

## subsumes

plans/shadow.md § ShadowCatalog cache + invalidation + SchemaEvent emission

## differentiates from

architecture/shadow_communication.svg shows three CHANNELS (libpq, walsender, restore_command). this diagram shows ShadowCatalog INTERNAL state. don't duplicate

## concept

ShadowCatalog internal state: LRU caches keyed by_filenode and by_oid + generation counter + libpq tokio_postgres client + invalidation_drain task + schema_event_tx mpsc. relation_at(rfn, lsn) gates on shadow's pg_last_wal_replay_lsn; cache miss fires libpq SELECT, diffs vs prior, emits Added / Changed / Dropped SchemaEvent

## clusters

| id | label | purpose |
|---|---|---|
| query | relation lookup path | relation_at → gate → cache hit/miss |
| cache | LRU + generation | by_filenode + by_oid + gen counter |
| invalid | invalidation drain | CatalogTracker ping → bump gen |
| emit | schema event | diff → SchemaEvent → tx → consumers |
| reconnect | libpq resilience | drop → reconnect loop → bump gen |

## key nodes

- relat: "relation_at(rfn, lsn)\nwait_for_replay" — #4D4D28
- lru_rfn: "LRU 4096\nby_filenode" — #4D4D28
- lru_oid: "LRU 4096\nby_oid" — #4D4D28
- gen: "generation counter" — #4D4D28, shape=cylinder
- libpq: "tokio_postgres::Client\nSELECT pg_class /\npg_attribute / pg_type" — #4D4D28
- diff: "diff vs prior\nclassify Added/\nChanged/Dropped" — #4D4D28
- evtx: "schema_event_tx\n(unbounded mpsc)" — #4D4D28, shape=parallelogram
- inval: "invalidation_drain task" — #4D4D28
- track_in: "← CatalogTracker\n(pg_class write LSN)" — #4D3A28, shape=note (cross-component entry)
- reconnect: "auto-reconnect loop\nexponential backoff" — #4D4D28
- shd: "shadow PG\npg_last_wal_replay_lsn()" — #3D4128, cylinder
- xactbuf_out: "→ XactBuffer\n(lsn-stamped event)" — #4D4128, shape=note (cross-component exit)
- ddl_out: "→ DdlApplicator\n(separate CH TCP)" — #5D4628, shape=note

## key edges

| from | to | color | style | label |
|---|---|---|---|---|
| relat | lru_rfn | default | solid | check |
| lru_rfn | libpq | default | dashed | miss |
| libpq | shd | #CBA85E, penwidth=2, dir=both | solid | SELECT |
| libpq | diff | default | solid | row(s) |
| diff | lru_rfn | default | solid | populate |
| diff | lru_oid | default | solid | populate |
| diff | evtx | #CBA85E | dashed | event |
| gen | lru_rfn | default | dashed, constraint=false | invalidate |
| track_in | inval | #CBA85E | dashed | bump |
| inval | gen | default | solid | ↑ |
| relat | shd | #BD8183 | dotted, constraint=false | apply_lsn gate |
| reconnect | libpq | default | dashed | reset |
| reconnect | gen | default | dashed, constraint=false | bump on reconnect |
| evtx | xactbuf_out | #CBA85E | dashed | drain interleave |
| evtx | ddl_out | #CBA85E | dashed | apply |

## legend rows

- node-fill key (ShadowCatalog, shadow PG, cross-component pointers)
- edge-color key (libpq query, walsender gate, schema event)
- SchemaEvent variants subtable:
  - Added → CREATE TABLE
  - Changed → ADD COL / DROP COL / RENAME / type narrow
  - Dropped → DROP TABLE / DROP COL after grace

## layout hints

- rankdir=LR
- cache cluster wide so LRU + gen + diff sit side-by-side
- invalidation drain anchored top of cache cluster, feeds gen down

## quality bar

- gen → LRU invalidation edge clearly dashed, doesn't fight the populate edges
- reconnect cluster off to the side, doesn't deform main flow
- shadow PG cylinder only once (libpq and apply_lsn gate both touch it)
