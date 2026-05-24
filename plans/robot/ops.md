# robot:ops

## sources of truth

- plans/ops.md
- src/cursor.rs
- src/preflight.rs
- src/retention.rs (if exists)
- src/metrics.rs (if exists)

## subsumes

plans/ops.md § "Cursor file" + "Slot advance" + apply-lag metric

## concept

10s status loop reads ack-lsn from xact buffer (drain_lsn, emitter_ack_lsn) + shadow_replay_lsn (from walsender server's recv'd 'r' standby-status) → writes cursor.bin (atomic rename, CRC32C) → computes slot_advance = min(shadow_replay, emitter_ack) → SourceFeed::send_status emits (write, flush, apply) triple to source slot. Retention task GCs segments past slot_advance. Metrics export apply-lag

## clusters

| id | label | purpose |
|---|---|---|
| inputs | LSN sources | drain/ack/replay |
| status | status loop (10s) | compute + write cursor + send status |
| cursor_file | cursor.bin durability | atomic rename + CRC32C |
| feedback | source slot advance | standby-status triple |
| retain | retention | segment GC |
| metrics | Prom export | apply-lag + cursor gauges |

## key nodes

- ack_in: "← xact buffer\n(drain_lsn, emitter_ack_lsn)" — #4D4128, shape=note
- statrx_in: "← walsender server\n(shadow flush/apply LSN\nfrom 'r' frames)" — #5D3F40, shape=note
- loop: "status_loop task\n(tick 10s)" — #4D3A28
- compute: "compute slot_advance\n= min(shadow_replay,\n  emitter_ack)" — #4D3A28
- cursor_write: "cursor::write\natomic rename\nCRC32C" — #4D3A28
- cursor_file: "cursor.bin\n(see layout legend)" — #4D3850, shape=note
- send_status: "SourceFeed::send_status\nwrite / flush / apply" — #4D3A28
- src_slot: "source PG\npg_replication_slots" — #3D3D54, cylinder
- retain: "retention task\ndelete segs older than\nmin(slot_advance, window)" — #4D3A28
- outdir: "out/<seg>\nfiltered WAL" — #4D3850, shape=note
- metrics: "Prom endpoint\nwalshadow_shadow_apply_lag_bytes\nwalshadow_shadow_apply_lag_seconds\ncursor gauges" — #4D3A28, shape=cylinder
- preflight: "preflight validators\n(boot only,\nnot in tick loop)" — #4D3A28, shape=note

## key edges

| from | to | color | style | label |
|---|---|---|---|---|
| ack_in | loop | default | solid | |
| statrx_in | loop | default | solid | |
| loop | compute | default | solid | |
| compute | cursor_write | default | solid | |
| cursor_write | cursor_file | #b380b0, penwidth=2 | dotted | atomic rename |
| compute | send_status | default | solid | |
| send_status | src_slot | #A1A9CC | dashed | standby triple |
| compute | retain | default | dashed, constraint=false | advance bound |
| retain | outdir | #6E6963 | dashed | unlink |
| loop | metrics | default | dashed, constraint=false | export |
| preflight | loop | default | dashed, constraint=false | (boot-time only, not tick) |

## legend rows

- node-fill key
- edge-color key (cursor magenta dotted, source replication, filesystem, libpq absent here)
- cursor.bin layout subtable (CONFIRM FIELD COUNT FROM src/cursor.rs — plans/INDEX.md says "five-field" but plans/ops.md says "6 LSNs"; cursor.rs is authoritative):
  - u16 magic
  - u16 version
  - LSN fields (count + names per cursor.rs)
  - u32 CRC32C trailer
  - total 56 bytes
- standby-status triple subtable:
  - write_lsn = current pump position
  - flush_lsn = slot_advance = min(shadow_replay, emitter_ack)
  - apply_lsn = flush_lsn

## layout hints

- rankdir=TB
- compute at center; cursor_file off to one side; src_slot opposite
- retain + metrics as side branches off compute / loop

## quality bar

- cursor write edge (magenta dotted) reads as durability path
- slot advance edge clearly back to source PG, not confused with metrics export
- preflight noted as boot-only (not in tick loop) — use dashed + label or place outside main loop cluster

## known discrepancy

plans/INDEX.md describes cursor as "five-field"; plans/ops.md describes "6 LSNs". Reflect what `src/cursor.rs` actually defines. Reconcile prose in INDEX.md / ops.md in a follow-up edit; this spec defers to source
