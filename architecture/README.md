### 1. Overview — Postgres → walshadow → ClickHouse

High-level pipeline. Shadow PG runs as catalog-replay sidecar fed by
walshadow's walsender; filtered segments under `out/` serve as the archive
fallback. The CH wire is now a held-open INSERT (multi-xact,
deadline-flushed; PHASE14) and walshadow also pushes DDL through a
second CH connection (PHASE15).

![overview](overview.svg)

### 2. Internals — pipeline, taps & caches

Hot path runs top→bottom; ancillaries (catalog cache, walsender server,
disk artifacts) sit off to the right with `constraint=false` edges so they
don't pull the main column off axis. The `QueueingRecordSink` between
fan-out and decoder (POST13zerocopy) keeps the decoder's
`wait_for_replay` off the pump task so the walsender wire never stalls
behind it. The CH cluster now bundles the steady-state emitter together
with the PHASE15 `DdlApplicator` (separate CH TCP) and the `type_bridge`.

![internals](internals.svg)

### 3. Shadow communication — three channels

How walshadow talks to shadow PG: ① libpq catalog queries, ② walsender
wire at record cadence (PHASE13), ③ `restore_command` archive fallback,
plus the one-shot BASE_BACKUP land for greenfield bootstrap. PHASE15's
schema-event flow is derived off channel ① (cache miss → diff →
`SchemaEvent` → `DdlApplicator` → CH) and stays inside walshadow.

![shadow communication](shadow_communication.svg)

### 4. Bootstrap timeline — greenfield in five phases

Catalog seed → BASE_BACKUP pump (MultiplexSink fan-out) → drain to CH →
shadow handoff → cursor + WAL pump start. The bootstrap-time emitter is
transitional: held-open INSERT for throughput, force-closed at end, no
`DdlApplicator` wired. Each phase is a labelled cluster; node fill
colour-codes the actor.

![bootstrap timeline](timeline_bootstrap.svg)

### 5. Streaming timeline — one record's journey

Steady-state hot path, left→right. The bytes path (③→④) stays on the
pump task; the decoder path (③→④'→⑤→⑥) crosses the
`QueueingRecordSink` hand-off so it can wait on shadow without parking
the wire. CH ⑥ is the held-open INSERT (PHASE14): `send_query` once per
table, `send_data(Some)` per block, `send_data(None)` only when the
`flush_timeout` deadline trips (or on close).

![streaming timeline](timeline_streaming.svg)

### 6. Restart timelines — three scenarios

Side-by-side columns: A. clean SIGTERM, B. kill -9 mid-stream
(validated by the PHASE14 `phase14_kill_restart` drill), C. WAL
overflow → re-bootstrap. Includes the cursor.bin five-field reference
table — layout unchanged since PHASE11.

![restart timelines](timeline_restart.svg)

## Render

```sh
for f in *.dot; do dot -Tsvg "$f" -o "${f%.dot}.svg"; done
```

## Key references

| diagram detail | source |
|---|---|
| PHASE15 (catalog-event channel, CH DDL applicator) | [`plans/PHASE15.md`](../plans/PHASE15.md) |
| PHASE14 (held-open INSERT, TRUNCATE, subxact rollback, apply-lag) | [`plans/phase14/PLAN.md`](../plans/phase14/PLAN.md) |
| POST13zerocopy (`QueueingRecordSink`, pump ↔ decoder decoupling) | [`plans/POST13zerocopy.md`](../plans/POST13zerocopy.md) |
| PRE15 cleanup | [`plans/PRE15.md`](../plans/PRE15.md) |
| PHASE13 (streaming-fed shadow) | [`plans/PHASE13.md`](../plans/PHASE13.md) |
| PHASE12 (greenfield bootstrap) | [`plans/PHASE12.md`](../plans/PHASE12.md) |
| PHASE11 (durable cursor) | [`plans/PHASE11.md`](../plans/PHASE11.md) |
| PHASE6 disk-spill | [`plans/PHASE6disk.md`](../plans/PHASE6disk.md) |
