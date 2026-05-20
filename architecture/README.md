### 1. Overview — Postgres → walshadow → ClickHouse

High-level pipeline. Shadow PG runs as catalog-replay sidecar fed by
walshadow's walsender; filtered segments under `out/` serve as the archive
fallback.

![overview](overview.svg)

### 2. Internals — pipeline, taps & caches

Hot path runs top→bottom; ancillaries (catalog cache, walsender server,
disk artifacts) sit off to the right with `constraint=false` edges so they
don't pull the main column off axis.

![internals](internals.svg)

### 3. Shadow communication — three channels

How walshadow talks to shadow PG: ① libpq catalog queries, ② walsender
wire at record cadence (Phase 13), ③ `restore_command` archive fallback,
plus the one-shot BASE_BACKUP land for greenfield bootstrap.

![shadow communication](shadow_communication.svg)

### 4. Bootstrap timeline — greenfield in five phases

Catalog seed → BASE_BACKUP pump (MultiplexSink fan-out) → drain to CH →
shadow handoff → cursor + WAL pump start. Each phase is a labelled cluster;
node fill colour-codes the actor.

![bootstrap timeline](timeline_bootstrap.svg)

### 5. Streaming timeline — one record's journey

Steady-state hot path, left→right. Seven phases: ingress, filter+rewrite,
fan-out, shadow apply (hot wire), decoder gate clear, commit drain to CH,
async durability (off hot path).

![streaming timeline](timeline_streaming.svg)

### 6. Restart timelines — three scenarios

Side-by-side columns: A. clean SIGTERM, B. kill -9 mid-stream, C. WAL
overflow → re-bootstrap. Includes the cursor.bin five-field reference
table.

![restart timelines](timeline_restart.svg)

## Render

```sh
for f in *.dot; do dot -Tsvg "$f" -o "${f%.dot}.svg"; done
```

## Key references

| diagram detail | source |
|---|---|
| Phase 13 (streaming-fed shadow) | [`plans/PHASE13.md`](../plans/PHASE13.md) |
| Phase 12 (greenfield bootstrap) | [`plans/PHASE12.md`](../plans/PHASE12.md) |
| Phase 11 (durable cursor) | [`plans/PHASE11.md`](../plans/PHASE11.md) |
| Phase 6 disk-spill | [`plans/PHASE6disk.md`](../plans/PHASE6disk.md) |
