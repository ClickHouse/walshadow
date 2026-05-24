# robot:decoder

## sources of truth

- plans/decoder.md
- src/heap_decoder.rs (confirm name via `ls src/ | grep -i heap`)
- src/codecs.rs
- src/main_data.rs

## subsumes

plans/decoder.md § "Entry point" + "HeapOp variants" + tier overview. detailed tier matrix table KEPT in prose

## concept

heap WAL record + RelDescriptor → dispatch by xl_info → INSERT / UPDATE / HOT_UPDATE / DELETE / MULTI_INSERT / TRUNCATE (intercept) → decode_tuple_payload (header + bitmap + cols) → per-attribute tier dispatch → DecodedHeap to xact buffer

## clusters

| id | label | purpose |
|---|---|---|
| in | record + descriptor in | rfn → RelDescriptor resolved |
| dispatch | by xl_info | branch to op variant |
| tuple | decode_tuple_payload | header + bitmap + cols |
| tier | type matrix | tier 1/2/3 + PgPending |
| out | DecodedHeap | xid + source_lsn + op + new/old |
| trunc | truncate side path | pg_class OID list intercept |
| multi | multi_insert side path | ntuples loop |

## key nodes

- in_rec: "heap record\nrfn = block[0].location" — #3D3D54, shape=note
- in_desc: "RelDescriptor\n(ShadowCatalog::relation_at)" — #4D4D28
- truncate: "BufferingDecoderSink::handle_truncate\n(intercept pre-decode)\n→ pg_class OID list" — #4D4128
- truncate_resolve: "relation_by_oid\n× nrelids" — #4D4D28
- dispatch: "dispatch by xl_info\n0x00 INSERT\n0x10 DELETE\n0x20 UPDATE\n0x40 HOT_UPDATE\n0x50 MULTI_INSERT" — #4D4128
- multi: "decode_multi_insert\nwalk xl_heap_multi_insert\nntuples loop" — #4D4128
- payload: "decode_tuple_payload\nxl_heap_header (5)\nbitmap\ncols [t_hoff]" — #4D4128
- bitmap: "null bitmap walk\natt_align_nominal" — #4D4128
- tier1: "Tier 1 fixed-width\nbool/int/float\ndate/time/uuid/name" — #4D4128
- tier2: "Tier 2 varlena\nbytea / text / json\n(1-byte / 4-byte / TOAST ptr)" — #4D4128
- tier3: "Tier 3 codecs\nnumeric / inet / cidr\ninterval / json" — #4D4128
- pending: "PgPending\n→ oracle path" — #4D4128
- missing: "read-time default\nmissing_value_for(att)\natthasmissing path" — #4D4128
- replid: "replica identity\nDefault/Nothing/Full/UsingIndex" — #4D4128, shape=note
- out: "DecodedHeap\n{ rfn, xid, source_lsn,\n  op, new, old }" — #4D4128, shape=cylinder

## key edges

| from | to | color | style | label |
|---|---|---|---|---|
| in_rec | in_desc | #CBA85E | dashed | resolve |
| in_rec | dispatch | default | solid | |
| in_rec | truncate | default | dashed | xl_info=0x30 |
| truncate | truncate_resolve | #CBA85E | dashed | |
| truncate_resolve | out | default | solid | fanout |
| dispatch | payload | default | solid | single-tuple |
| dispatch | multi | default | solid | 0x50 |
| multi | payload | default | solid | per-tuple |
| payload | bitmap | default | solid | |
| bitmap | tier1 | default | solid | typlen > 0 |
| bitmap | tier2 | default | solid | typlen = -1 |
| bitmap | tier3 | default | solid | tier3 OID |
| bitmap | pending | default | dashed | unsupported |
| bitmap | missing | default | dashed | natts < attnum |
| replid | payload | default | dashed, constraint=false | old-tuple shape |
| tier1 | out | default | solid | |
| tier2 | out | default | solid | |
| tier3 | out | default | solid | |
| pending | out | default | solid | raw bytes |
| missing | out | default | solid | |

## legend rows

- node-fill key
- edge-color key (libpq resolve, default tuple decode)
- tier matrix subtable:
  - Tier 1 → fixed width, inline decode
  - Tier 2 → varlena header parse, body dispatch by OID
  - Tier 3 → codecs.rs (numeric/inet/cidr/interval/json)
  - PgPending → oracle round-trip
- HeapOp xl_info code subtable (0x00 INSERT … 0x50 MULTI_INSERT, 0x30 TRUNCATE intercepted)

## layout hints

- rankdir=TB
- truncate path off to one side (left or right of dispatch)
- multi_insert side branch off dispatch joins payload (don't duplicate payload node)
- tier1/tier2/tier3/pending/missing rank-aligned below bitmap so out converges cleanly

## quality bar

- 5+ tier outputs all reaching `out` without crossing
- truncate intercept clearly distinct (pre-dispatch)
- replid annotation hangs off payload without deforming dispatch column
