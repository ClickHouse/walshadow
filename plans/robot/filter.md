# robot:filter

## sources of truth

- plans/filter.md
- src/filter.rs
- src/catalog_tracker.rs
- src/rewrite.rs

## subsumes

plans/filter.md § "Filter contract" + "Rewrite over fork"

## concept

per-record decision: WAL chunk → walker stitches records across pages → classify (keep / drop / empty-reclass) → rewrite-in-place at byte_ranges + CRC32C recompute, OR NOOP-of-equal-length stamp. CatalogTracker feeds whitelist live from pg_class heap writes + RM_RELMAP_ID

## clusters

| id | label | purpose |
|---|---|---|
| ingress | WAL chunk source | WalChunk (start_lsn, bytes) entry |
| pipeline | filter pipeline (sync, record cadence) | walker → decide → rewrite |
| track | CatalogTracker | relfilenode whitelist + relmap |
| emit | segment buffer | 16 MiB rewritten image out |

## key nodes (id, label, fill)

- chunks: "WalChunk (start_lsn, bytes)" — #3D3D54, parallelogram
- walker: "StreamingWalker\npage state machine\nrecord stitch" — #4D3A28
- decide: "Filter::decide\nKeep catalog\nDrop user-heap\nEmpty → reclass" — #4D3A28
- track: "CatalogTracker\nrelfilenode set\n(relmap + pg_class)" — #4D3A28
- empty_md: "main_data reclass\nNEW_CID, BTREE_REUSE_PAGE" — #4D3A28, shape=note
- rewrite: "rewrite::noop_replace\nin-place at byte_ranges\nCRC32C recompute" — #4D3A28
- noop: "XLOG_NOOP synth\nequal xl_tot_len" — #4D3A28
- segbuf: "16 MiB segment buffer" — #4D3A28, shape=cylinder

## key edges

| from | to | color | style | label | notes |
|---|---|---|---|---|---|
| chunks | walker | #A1A9CC | solid, penwidth=2 | | source frame |
| walker | decide | default | solid | CompletedRecord | |
| decide | rewrite | default | solid | keep / partial | |
| decide | noop | default | dashed | drop all blocks | |
| decide | empty_md | default | dashed | Empty class | constraint=false |
| empty_md | track | default | dashed | rfn lookup | constraint=false |
| track | decide | #CBA85E | dashed, dir=both, constraint=false | whitelist | |
| rewrite | segbuf | default | solid | | |
| noop | segbuf | default | solid | | |

## legend rows

- node-fill key (filter cluster fill, on-disk artifact, source)
- edge-color key (source frame, libpq feedback)
- rmgr keep policy subtable:
  - HEAP / HEAP2 → kept iff rfn in catalog set
  - BTREE → kept iff relation is catalog index
  - RELMAP → all (shared-catalog rewrites)
  - XACT / CLOG / MULTIXACT / STANDBY → all
  - XLOG → checkpoint, nextoid, parameter-change only
  - everything else → drop

## layout hints

- rankdir=TB (record flows top-down)
- track cluster off to the side with constraint=false feedback edges
- empty_md is a side branch off decide, not a main column hop

## quality bar

- track cluster doesn't push decide off main column
- noop / rewrite siblings rank-aligned so segbuf joins cleanly
- rmgr table in legend fits within graph width
