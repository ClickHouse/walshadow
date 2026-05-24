# robot:xact

## sources of truth

- plans/xact.md
- src/xact_buffer.rs
- src/spill.rs

## subsumes

plans/xact.md § "Buffer shape" + "Eviction policy" + "Spill backend" + "Drain shape"

## concept

per-xid buffer between decoder and commit-drain observer. insert tuple/chunk → maybe evict largest → spill to per-xid file → on COMMIT, k-way merge spill + in_mem across (top + subxids) in source_lsn order → observer.on_tuple → observer.on_xact_end

## clusters

| id | label | purpose |
|---|---|---|
| in | decoder fan-in | DecodedHeap + ToastChunk inputs |
| buf | XactBuffer state | inflight HashMap + bytes_in_memory |
| state | per-xid XactState | in_mem + spill writer + catalog_events |
| evict | eviction policy | maybe_evict largest |
| spill_io | spill backend | per-xid file format |
| drain | commit drain | k-way merge → observer |
| subx | subxact tracker | parent/children + ASSIGNMENT + commit subxid list |
| out | observer / cursor | CommittedTuple + xact_end ack |

## key nodes

- dec_in: "← decoder\nDecodedHeap +\nToastChunk" — #4D4128, shape=note
- absorb: "XactBuffer::absorb(xid)" — #4D4128
- inflight: "inflight HashMap<u32, XactState>\nbytes_in_memory ↑" — #4D4128, shape=cylinder
- state: "XactState {\n  first_lsn,\n  in_mem: Vec<SpillEntry>,\n  spill: Option<SpillWriter>,\n  catalog_events,\n}" — #4D4128, shape=record
- evict: "maybe_evict\nbytes > 64 MiB?\npick largest xact" — #4D4128
- spwrite: "SpillWriter\nappend [tag u8 | len u32 | body]" — #4D4128
- spfile: "{spill}/xid-{xid:010}-{first_lsn:016X}.bin\nmagic \"WS\", ver u16\nentries" — #4D3850, shape=note
- subx: "SubxactTracker\nparent: HashMap<u32,u32>\nchildren: HashMap<u32,Vec<u32>>" — #4D4128
- subx_in: "← XLOG_XACT_ASSIGNMENT\n(xtop, nsub, xsub[])" — #3D3D54, shape=note
- commit: "XLOG_XACT_COMMIT\n+ subxid list\n+ catalog_events" — #3D3D54, shape=note
- drain_pull: "drain: pull (top, ..subxids)\nfrom inflight" — #4D4128
- spread: "SpillReader::next()\nstream entries" — #4D4128
- kmerge: "k-way merge\nlinear-scan head pick\nk ≤ 4 typically" — #4D4128
- toast: "TOAST reassembly\nHashMap<(relid, valid),\n  BTreeMap<seq, chunk>>" — #4D4128
- evbus: "catalog_events\nlsn-stamped insertion" — #4D4D28
- observer: "observer.on_tuple\nobserver.on_xact_end" — #5D4628, shape=note
- abort: "XLOG_XACT_ABORT\ndrop xid + subxids\nunlink spill" — #3D3D54, shape=note

## key edges

| from | to | color | style | label |
|---|---|---|---|---|
| dec_in | absorb | default | solid | |
| absorb | inflight | default | solid | |
| inflight | state | default | dashed | per-xid |
| absorb | evict | default | solid | after each absorb |
| evict | spwrite | default | solid | flush in_mem |
| spwrite | spfile | #6E6963 | solid | append fsync |
| subx_in | subx | default | solid | parse_xact_assignment |
| subx | drain_pull | default | dashed, constraint=false | walk children |
| commit | drain_pull | default | solid | |
| drain_pull | spread | default | solid | per-xid file |
| spread | spfile | #6E6963 | dashed | read |
| drain_pull | kmerge | default | solid | in_mem head |
| spread | kmerge | default | solid | spill head |
| kmerge | toast | default | dashed | reassemble varlena |
| kmerge | observer | default | solid, penwidth=2 | per-tuple |
| evbus | kmerge | #CBA85E | dashed | lsn-tie precedence |
| abort | inflight | default | dashed, constraint=false | discard |
| abort | spfile | #6E6963 | dashed | unlink |

## legend rows

- node-fill key (xact buffer, on-disk, source PG events, ShadowCatalog event)
- edge-color key (filesystem dashed, libpq schema event, control flow)
- drain order subtable:
  - on source_lsn ties: catalog event sorts BEFORE heap (PG writes catalog before dependent heap)
  - k-way merge head pick: linear scan (k ≤ 4); heap not worth it
- spill file format subtable: magic "WS" 2B + ver u16 (current=2) + repeating [tag u8 | len u32 | body]
- HeapOp tag encoding: 0 Insert / 1 Update / 2 HotUpdate / 3 Delete / 4 Truncate

## layout hints

- rankdir=TB
- evict + spill_io clusters off to one side; commit drain forms main vertical column
- subx tracker as side cluster feeding drain_pull
- catalog event bus crosses laterally into kmerge, constraint=false

## quality bar

- spill round-trip (write → file → read) visually traceable
- subx tracker feeds drain without overlapping spill IO
- evbus → kmerge edge doesn't entangle with main merge edges
