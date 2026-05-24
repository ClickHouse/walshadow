### 1. Overview — Postgres → walshadow → ClickHouse

High-level pipeline. Shadow PG runs as catalog-replay sidecar fed by
walshadow's walsender; filtered segments under `out/` serve as archive
fallback. CH wire is a held-open INSERT (multi-xact, deadline-flushed);
walshadow pushes DDL through a second CH connection.

![overview](overview.svg)

### 2. Internals — pipeline, taps & caches

Hot path runs top→bottom; ancillaries (catalog cache, walsender server,
disk artifacts) sit off to the right with `constraint=false` edges so
they don't pull the main column off axis. `QueueingRecordSink` between
fan-out and decoder keeps the decoder's `wait_for_replay` off the pump
task so the walsender wire never stalls behind it. CH cluster bundles
the steady-state emitter together with `DdlApplicator` (separate CH
TCP) and `type_bridge`.

![internals](internals.svg)

### 3. Shadow communication — three channels

How walshadow talks to shadow PG: ① libpq catalog queries, ② walsender
wire at record cadence, ③ `restore_command` archive fallback, plus the
one-shot BASE_BACKUP land for greenfield bootstrap. Schema-event flow
derives off channel ① (cache miss → diff → `SchemaEvent` →
`DdlApplicator` → CH) and stays inside walshadow.

![shadow communication](shadow_communication.svg)

### 4. Bootstrap timeline — greenfield in five phases

Catalog seed → BASE_BACKUP pump (MultiplexSink fan-out) → drain to CH →
shadow handoff → cursor + WAL pump start. Bootstrap-time emitter is
transitional: held-open INSERT for throughput, force-closed at end, no
`DdlApplicator` wired. Each phase is a labelled cluster; node fill
colour-codes the actor.

![bootstrap timeline](timeline_bootstrap.svg)

### 5. Streaming timeline — one record's journey

Steady-state hot path, left→right. Bytes path (③→④) stays on the pump
task; decoder path (③→④'→⑤→⑥) crosses `QueueingRecordSink` so it can
wait on shadow without parking the wire. CH ⑥ is held-open INSERT:
`send_query` once per table, `send_data(Some)` per block,
`send_data(None)` only when `flush_timeout` deadline trips or on close.

![streaming timeline](timeline_streaming.svg)

### 6. Restart timelines — three scenarios

Side-by-side columns: A. clean SIGTERM, B. kill -9 mid-stream
(validated by `phase14_kill_restart` drill), C. WAL overflow →
re-bootstrap. Includes cursor.bin six-field reference table.

![restart timelines](timeline_restart.svg)

## Render

```sh
for f in *.dot; do dot -Tsvg "$f" -o "${f%.dot}.svg"; done
```

## Key references

| diagram detail | source |
|---|---|
| catalog-event channel + CH DDL applicator | [`plans/shadow.md`](../plans/shadow.md), [`plans/emitter.md`](../plans/emitter.md) |
| held-open INSERT, TRUNCATE, subxact rollback, apply-lag | [`plans/emitter.md`](../plans/emitter.md), [`plans/xact.md`](../plans/xact.md), [`plans/ops.md`](../plans/ops.md) |
| `QueueingRecordSink`, pump ↔ decoder decoupling | [`plans/source.md`](../plans/source.md) |
| streaming-fed shadow | [`plans/shadow.md`](../plans/shadow.md), [`plans/source.md`](../plans/source.md) |
| greenfield bootstrap | [`plans/bootstrap.md`](../plans/bootstrap.md) |
| durable cursor | [`plans/ops.md`](../plans/ops.md) |
| xact buffer + disk spill | [`plans/xact.md`](../plans/xact.md) |
