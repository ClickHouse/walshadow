### 1. Overview — Postgres → walshadow → ClickHouse

High-level pipeline. Shadow PG runs as catalog-replay sidecar fed by
walshadow's walsender; filtered segments under `out/` serve as archive
fallback. CH rows buffer across xacts and seal as complete INSERTs
(budget / deadline) shipped over an N-connection inserter pool;
walshadow pushes DDL through its own CH connection.

![overview](overview.svg)

### 2. Internals — pipeline, taps & caches

Hot path runs top→bottom; ancillaries (catalog cache, walsender server,
disk artifacts) sit off to the right with `constraint=false` edges so
they don't pull the main column off axis. `QueueingRecordSink` between
fan-out and decoder keeps the decoder's `wait_for_replay` off the pump
task so the walsender wire never stalls behind it. CH cluster is the
pipeline tail — decode pool ×M, `InsertBatcher`, inserter pool ×N, ack
collector — plus `DdlApplicator` (own CH connection) and
`type_bridge`. TOAST side path persists TID-keyed births/tombstones,
serves as-of fetches, and applies lifecycle barriers.

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
shadow handoff → cursor + WAL pump start. Drain feeds the same shared
insert tail streaming uses (batcher + inserter pool + ack collector),
one ack seq per rfn flip, `tail.finish` seals + waits durable at end;
TOAST rows flush before deferred external values resolve; no
`DdlApplicator` wired. Each phase is a labelled cluster; node fill
colour-codes actor.

![bootstrap timeline](timeline_bootstrap.svg)

### 5. Streaming timeline — one record's journey

Steady-state hot path, left→right. Bytes path (③→④) stays on the pump
task; decoder path (③→④'→⑤→⑥) crosses `QueueingRecordSink` so it can
wait on shadow without parking the wire. ⑥ is the parallel pipeline:
reorder assigns a dense seq per commit, decode pool routes rows, the
batcher buffers per table across xacts and seals one complete INSERT
per budget/deadline window, inserter pool ships N in flight; the ack
collector advances `emitter_ack_lsn` on contiguous-done prefix. Reorder
persists TOAST births/tombstones before commit publication; decode uses
in-xact maps then as-of mirror fetch.

![streaming timeline](timeline_streaming.svg)

### 6. Restart timelines — three scenarios

Side-by-side columns: A. clean SIGTERM, B. kill -9 mid-stream
(validated by `tests/kill_restart.rs` drill), C. WAL overflow →
source/archive/source fallback or operator resolution. Includes cursor.bin six-field reference table.
`toast_retires.bin` survives xid-spill cleanup and flushes due mirror
retirements from persisted resume floor at standup.

![restart timelines](timeline_restart.svg)

## Component diagrams

Focused views for components with load-bearing topology. Embedded inline
in matching plan docs. Render alongside six system views above.

| component | source | embedded in |
|---|---|---|
| filter | [`filter.dot`](filter.dot) | [`plans/filter.md`](../plans/filter.md) |
| source | [`source.dot`](source.dot) | [`plans/source.md`](../plans/source.md) |
| shadow | [`shadow.dot`](shadow.dot) | [`plans/shadow.md`](../plans/shadow.md) |
| decoder | [`decoder.dot`](decoder.dot) | [`plans/decoder.md`](../plans/decoder.md) |
| xact | [`xact.dot`](xact.dot) | [`plans/xact.md`](../plans/xact.md) |
| TOAST | [`toast.dot`](toast.dot) | [`plans/TOAST.md`](../plans/TOAST.md) |
| emitter | [`emitter.dot`](emitter.dot) | [`plans/emitter.md`](../plans/emitter.md) |
| bootstrap | [`bootstrap.dot`](bootstrap.dot) | [`plans/bootstrap.md`](../plans/bootstrap.md) |
| ops | [`ops.dot`](ops.dot) | [`plans/ops.md`](../plans/ops.md) |
| oracle | [`oracle.dot`](oracle.dot) | [`plans/oracle.md`](../plans/oracle.md) |

## Regenerating a diagram

Each `<comp>.dot` carries its own regeneration spec as a header comment
(sources of truth, `plans/` section subsumed, quality bar). Shared style
— palette, edge channels, legend conventions — lives in
[`palette.md`](palette.md).

To regenerate `architecture/<comp>.svg`:
1. read [`palette.md`](palette.md) for shared style invariants
2. read the regen-spec header in `<comp>.dot` (sources of truth, subsumes, quality bar)
3. read `plans/<comp>.md` for current implementation truth, plus the cited `src/` files as accuracy anchor
4. edit `<comp>.dot`, render (below), read the png, iterate until the header quality bar passes
5. if the `.svg` path changed, update the `plans/<comp>.md` embed

System-level diagrams (overview, internals, shadow_communication,
timeline_*) carry no per-comp spec — stable and visually saturated. Add
one only on the next material rewrite.

## Render

```sh
for f in *.dot; do dot -Tsvg "$f" -o "${f%.dot}.svg"; dot -Tpng "$f" -o "${f%.dot}.png"; done
```

## Key references

| diagram detail | source |
|---|---|
| catalog-event channel + CH DDL applicator | [`plans/shadow.md`](../plans/shadow.md), [`plans/emitter.md`](../plans/emitter.md) |
| atomic-seal INSERT, TRUNCATE, subxact rollback, apply-lag | [`plans/emitter.md`](../plans/emitter.md), [`plans/xact.md`](../plans/xact.md), [`plans/ops.md`](../plans/ops.md) |
| `QueueingRecordSink`, pump ↔ decoder decoupling | [`plans/source.md`](../plans/source.md) |
| streaming-fed shadow | [`plans/shadow.md`](../plans/shadow.md), [`plans/source.md`](../plans/source.md) |
| greenfield bootstrap | [`plans/bootstrap.md`](../plans/bootstrap.md) |
| durable cursor | [`plans/ops.md`](../plans/ops.md) |
| xact buffer + disk spill | [`plans/xact.md`](../plans/xact.md) |
| TOAST mirror, fetch, bootstrap, rewrite, retirement | [`plans/TOAST.md`](../plans/TOAST.md) |
