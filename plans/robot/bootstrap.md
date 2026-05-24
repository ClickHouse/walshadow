# robot:bootstrap

## sources of truth

- plans/bootstrap.md
- src/backup_source_direct.rs
- src/backup_source_object_store.rs
- src/backup_page_walk.rs
- src/backup_source.rs

## subsumes

plans/bootstrap.md § "MultiplexSink" / fan-out

## differentiates from

architecture/timeline_bootstrap.svg shows the 5-phase TIMELINE. this diagram shows the FAN-OUT STRUCTURE: BackupSource impls, MultiplexSink, simultaneous shadow-data-dir + CH writes

## concept

BASE_BACKUP greenfield: BackupSource yields (file_path, file_bytes); orchestrator + RelationResolver fans each file via MultiplexSink to (a) shadow data dir as raw bytes, AND (b) — for user-relation heaps only — PageWalkSink → block builder → bootstrap-time CH INSERT. At end_lsn, cursor hands off to streaming WAL pump; shadow standby starts replay

## clusters

| id | label | purpose |
|---|---|---|
| trait_src | BackupSource trait | two impls (direct, object_store) |
| orchestrate | backfill_bootstrap | RelationResolver + driver loop |
| multiplex | MultiplexSink fan-out | route per file type |
| shadow_dir | ShadowDataDirSink | raw bytes → {data_dir}/{path} |
| pagewalk | PageWalkSink | FPI restore + 2A page walk → block |
| ch_boot | bootstrap CH emitter | held-open INSERT, no DdlApplicator |
| handoff | end_lsn handoff | cursor + streaming pump start |

## key nodes

- src_direct: "BackupSourceDirect\ntokio-postgres BASE_BACKUP wire" — #3D3D54
- src_objs: "BackupSourceObjectStore\ns3/gcs prefetched tarball" — #3D3D54
- file_iter: "iter (file_path, bytes)" — #3D3D54, parallelogram
- relresolve: "RelationResolver\nrelfilenode → pg_class OID\n(from initial pg_class fetch)" — #4D4D28
- orch: "backfill_bootstrap\ndriver loop" — #4D4128
- mux: "MultiplexSink\nBackupSink trait" — #4D3340
- classify: "classify by\nkind / file path\n(catalog/index/heap/settings)" — #4D3340
- shadsink: "ShadowDataDirSink\nwrite raw bytes →\n{data_dir}/{relpath}" — #4D3340
- pagesink: "PageWalkSink\nfor kind in ('r','p')" — #4D3340
- fpi: "fpi.rs\nrestore_block_image\n(Pglz/Lz4/Zstd)" — #4D4128
- walk: "per-page tuple walk\n(like 2A decoder)" — #4D4128
- block: "BlockBuilder per relation\nNative columns +\nbootstrap synthetic" — #5D4628
- chinsert: "held-open INSERT\nforce-close at end_lsn\nno DdlApplicator" — #5D4628
- shaddir: "{shadow_data_dir}/..." — #4D3850, shape=note
- chsink: "ClickHouse" — #4D4128, cylinder
- handoff: "end_lsn reached\nwrite cursor +\nstart streaming pump" — #4D3A28
- pump_out: "→ source.svg pipeline\n(streaming WAL)" — #4D3A28, shape=note

## key edges

| from | to | color | style | label |
|---|---|---|---|---|
| src_direct | file_iter | #A1A9CC | dashed | BASE_BACKUP |
| src_objs | file_iter | #6E6963 | dashed | prefetched |
| file_iter | orch | default | solid | |
| orch | mux | default | solid | per file |
| mux | classify | default | solid | |
| classify | shadsink | default | solid | always |
| classify | pagesink | default | solid | kind ∈ ('r','p') |
| relresolve | classify | #CBA85E | dashed, constraint=false | rfn → OID |
| pagesink | fpi | default | solid | page image |
| fpi | walk | default | solid | restored |
| walk | block | default | solid | per tuple |
| block | chinsert | default | solid | |
| chinsert | chsink | #BF8C5F, penwidth=2 | solid | bootstrap rows |
| shadsink | shaddir | #6E6963 | solid | fsync |
| orch | handoff | default | dotted, constraint=false | end_lsn |
| handoff | pump_out | #b380b0 | dotted | cursor + start |

## legend rows

- node-fill key
- edge-color key (BASE_BACKUP wire blue, filesystem gray, bootstrap CH orange, cursor magenta)
- file routing rule subtable:
  - catalog files / pg_global / settings → ShadowDataDirSink only
  - user heap (kind ∈ {'r','p'}) → BOTH ShadowDataDirSink AND PageWalkSink
  - toast / index → ShadowDataDirSink only (CH replays heap from streaming WAL post-handoff)
- bootstrap emitter note: transitional, no DdlApplicator wired, force-close at end_lsn

## layout hints

- rankdir=TB (file flows top-down)
- two src impls side-by-side at top, both feed file_iter
- shadow_dir vs pagewalk side-by-side from classify (parallel paths)
- handoff cluster anchored at bottom

## quality bar

- "both" fan-out from classify visible (two edges, not one merged)
- RelationResolver feedback edge clearly secondary (dashed, constraint=false)
- bootstrap CH path distinct from steady-state emitter (annotated "no DdlApplicator")
