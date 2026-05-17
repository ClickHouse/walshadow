# PHASE8 — end-to-end DDL drill

Closes [Phase 8 of PLAN.md](PLAN.md#phase-8--end-to-end-ddl-drill).
First test that runs the full daemon chain against live processes:
source PG produces WAL → walshadow filters into shadow PG's
`restore_command` directory → shadow recovers the catalog →
[`ShadowCatalog`](../src/shadow_catalog.rs) hands descriptors to the
heap decoder → [`XactBuffer`](../src/xact_buffer.rs) commits drain into
the CH-Native [`Emitter`](../src/ch_emitter.rs) → a spawned
`clickhouse server` ingests the blocks → row count + payload aggregate
are diffed between source and CH.

Phases 1–7 shipped each component in isolation. Phase 8 is the first
seam that stitches them. It surfaced two production bugs the
component-level suites never exercised; both are fixed here.

## What landed

| item | files | tests |
|---|---|---|
| `WalStream::flush_current` + `close` dispatch order flipped — segment lands on disk *before* per-record dispatch fires | [`src/wal_stream.rs`](../src/wal_stream.rs) | `composite_sink_propagates_inner_error_and_short_circuits` in [`tests/multi_segment_filter.rs`](../tests/multi_segment_filter.rs) updated to match the new contract |
| `clickhouse-c-rs::Client::alloc` pinned in a `Box` so the C-side `c->al` pointer stays valid after `Client::init` returns | [`clickhouse-c-rs/src/client.rs`](../clickhouse-c-rs/src/client.rs) | exercised live by the Phase 8 drill (every `send_query` deref-uses `c->al`); prior to the fix the test SIGSEGV'd on first INSERT |
| `tests/phase8_e2e.rs` — single integration test driving source PG → walshadow filter → shadow PG (basebackup-bootstrapped standby) → decoder → xact buffer → emitter → spawned `clickhouse server`; verifies row count + post-UPDATE payload aggregate against source | [`tests/phase8_e2e.rs`](../tests/phase8_e2e.rs) | `phase8_insert_update_delete_replicates_to_clickhouse` — skipped silently when `initdb` / `pg_basebackup` / `clickhouse` aren't on `$PATH` |
| `plans/INDEX.md` extended with the Phase 8 entry | [`plans/INDEX.md`](INDEX.md) | doc-only |

Build clean on `cargo clippy --workspace --all-targets -- -D warnings`.

Test counts (local, PG 18.4 + ClickHouse 25.8):

- `cargo test --workspace --all-targets`: full suite green, +1 new
  test (`phase8_insert_update_delete_replicates_to_clickhouse`).
- Phase 8 drill end-to-end timing: ~1.5 s wall, dominated by CH
  server startup (~700 ms) and shadow PG basebackup (~150 ms).

Code size:

| component | LOC |
|---|---|
| `tests/phase8_e2e.rs` | ~660 |
| `src/wal_stream.rs` dispatch-order fix + docstring | ~10 net |
| `clickhouse-c-rs/src/client.rs` `Pin<Box<Allocator>>` shape + two `*self.client.alloc` derefs | ~5 net |
| `tests/multi_segment_filter.rs` assertion update + comment | ~5 net |

PLAN.md estimated 200 LOC of "test glue"; landed closer to 660 once
the basebackup / standby plumbing, CH server spawn + shutdown, and
the daemon sink chain are accounted for. The estimate underweighted
how much infrastructure the drill assembles in-process — every
upstream phase contributes some setup boilerplate.

## Bugs surfaced

### 1. Dispatch-order deadlock in `WalStream::flush_current`

Pre-Phase-8, `flush_current` dispatched filtered records to the
`RecordSink` chain *before* calling `segment_sink.on_segment` to write
the filtered bytes. The order was an early-return-on-error
optimisation: a sink-chain error skipped the segment write so shadow
never saw a partial / poisoned batch.

The decoder sink chain ([`BufferingDecoderSink`](../src/xact_buffer.rs))
calls `ShadowCatalog::relation_at(rfn, source_lsn)`, which blocks on
`pg_last_wal_replay_lsn() >= source_lsn`. Shadow's replay LSN can only
advance once shadow's `restore_command` finds the filtered segment
file. Pre-fix: that file was still in RAM, queued for write after the
per-record dispatch — the per-record dispatch was waiting on its own
caller. The 30 s default `replay_timeout` fired record-by-record,
`DecoderStats.replay_timeout` ticked up, the decoder skipped every
record, the test ended with `decoded=0` and CH empty.

Fix: swap the order. Write the filtered segment, *then* dispatch
records. Shadow's restore_command picks up the file (5 s default
poll interval; the Phase 8 test sets `wal_retrieve_retry_interval =
'100ms'` on shadow so the cycle is fast), shadow's recovery advances
its replay LSN, the decoder's `relation_at` returns. End-to-end
latency under the new order: <1 s from segment write to replay-LSN
advance to record dispatch on the same segment.

Trade-off: a record-sink error now leaves the segment on disk while
the rest of the records in that segment never dispatch. The
`WalStream` is already poisoned post-error per
[PRE5b10 item 4](pre5/PRE5b10.md); the recovery path is "drop the
poisoned stream and rebuild at the resume LSN", unchanged. The
existing test that pinned the old semantics
(`composite_sink_propagates_inner_error_and_short_circuits` in
[`tests/multi_segment_filter.rs`](../tests/multi_segment_filter.rs))
flipped its terminal assertion from `segs.segments.len() == 0` to
`== 1` plus a docstring explaining why.

### 2. `clickhouse-c-rs::Client` dangling alloc pointer

[`clickhouse-c-rs::Client::init`](../clickhouse-c-rs/src/client.rs)
took `alloc: Allocator` by value. Inside the function it called
`sys::chc_client_init(..., alloc.as_ptr(), ...)`, handing the C library
a `*const chc_alloc` pointer. The C client stashes that pointer
verbatim (`c->al = al;` in
[`clickhouse-c/clickhouse-client.h:389`](../clickhouse-c-rs/clickhouse-c/clickhouse-client.h)),
and every subsequent `alloc` / `realloc` / `free` derefs it. When
`Client::init` returned, the local `alloc` moved into `Self { raw,
alloc }` — fine for the value, but the *address* the C side held was
to the original stack slot. First `chc_client_send_query` triggered an
allocation through that stale pointer, hit a bogus function pointer
(0x1e000 in the seen coredump), SIGSEGV.

Component-level coverage didn't catch this because the
[`readme_quickstarts`](../clickhouse-c-rs/tests/readme_quickstarts.rs)
tests build a `Client` inside the same stack frame they use it from —
the original `Allocator` slot never goes away. Phase 8 builds the
`Emitter` in one function (`Emitter::new`) and uses it from another
(`route` called by `XactRecordSink::on_record` on a different tokio
worker), which is the exact "move the Allocator after init" shape that
trips the bug.

Fix: `Client.alloc` is now `Pin<Box<Allocator>>`. Address stability is
guaranteed by the heap allocation; the C client's `c->al` pointer
remains valid for the Client's lifetime. `Block::from_raw` /
`Exception::from_raw` consumers deref the box to keep their
`Allocator`-by-value signatures.

Audit followup: any other component that hands a `chc_alloc *` to C
and lets the source slot move (e.g. `chc_block_read`,
`chc_block_builder_init`) is potentially vulnerable to the same
pattern. `BlockBuilder::new` takes `alloc: Allocator` by value;
walshadow doesn't observe a crash there today because the builder is
constructed and used inline (`flush_block` calls `BlockBuilder::new`,
appends, sends, and drops without the builder ever leaving the stack
frame). Worth a pass when the emitter's mid-xact-flush path
materialises and the BlockBuilder lifetime stretches across awaits.

## Design decisions

### `pg_basebackup` for shadow bootstrap, not schema-only dump

`Shadow::apply_schema_dump` was the original Phase 3 bootstrap path
and works for the catalog-lifecycle tests because they construct
their own descriptors. The decoder, by contrast, looks up source
WAL records by their `RelFileLocator` via `pg_relation_filenode(oid)`
on shadow. The dump-and-replay approach creates fresh oids on shadow
that don't match the source's — every decoder probe would miss.

[BASEBACKUP.md](BASEBACKUP.md) sketches a Phase-6.5 `BASE_BACKUP`
protocol primitive that walshadow would use to bootstrap shadow.
Phase 8 doesn't need that surface yet: shelling out to
`pg_basebackup` is the canonical client implementation, ships with
every PG install, and produces a usable data dir without writing any
new Rust. The drill uses it via `Command::new("pg_basebackup")` with
`-X stream -c fast --no-sync`, then appends `port` /
`unix_socket_directories` / `listen_addresses` / `hot_standby`
overrides to the cloned `postgresql.conf` (last-wins) before
dropping `standby.signal` + `restore_command` for the daemon's
filter output dir.

`max_wal_senders` and `wal_level` are *not* overridden on shadow:
PG refuses to start a standby whose values are lower than the
primary's, and the basebackup-cloned conf already matches source.

### Segment rotation between basebackup and START_REPLICATION

After `pg_basebackup` completes, source's current xlogpos is somewhere
inside the segment whose bytes the basebackup already streamed to
shadow's local `pg_wal/`. Shadow's recovery prefers `restore_command`
over local `pg_wal/` per the standard PG ordering, but the post-
basebackup workload bytes inside that same segment aren't on the
filter output yet — walshadow ships them only when the segment seals.
A stale frozen copy in shadow's `pg_wal/` would race the daemon's
filtered shipment.

The drill issues `SELECT pg_switch_wal()` right after `pg_basebackup`
so the next segment starts fresh. The daemon attaches at the start
of that next segment via `IDENTIFY_SYSTEM` + `WalStream::align_down`.
Every byte of post-basebackup WAL flows through the filter end-to-end;
shadow's local pre-rotation copies are consistent because they
contain only basebackup-internal records.

### Multi-`-c` workload, autocommit-per-statement

The drill workload uses one `psql -c <stmt>` per statement rather
than chaining them in a single `-c` separated by semicolons. `psql -c`
with multiple semicolon-separated statements wraps the whole string
in one implicit transaction — the COMMIT would land *after*
`pg_switch_wal()`, in the next segment, which the test never ships.
Splitting into separate `-c` invocations puts each statement in its
own autocommit xact, and every COMMIT records lands in the same
segment as its heap records.

### `wal_retrieve_retry_interval = '100ms'` on shadow

Shadow's default 5 s retry interval is fine for production where WAL
flows continuously, but pessimistic for a one-shot test that ships a
single segment and waits for shadow to ingest it before the
decoder's `relation_at` 30 s timeout fires. 100 ms keeps the
restore_command poll tight; total drill wall time under 1.5 s.

### CH server lifecycle: `process_group(0)` + SIGTERM to the group

`clickhouse server` daemonises as a watchdog parent + worker child
pair. `child.kill()` (which sends SIGKILL via Rust's stdlib) signals
only the immediate child; the worker survives as an orphan owning
the test's TCP port. The drill puts CH in a fresh process group via
`Command::process_group(0)`, then `kill -TERM -<pgid>` signals the
whole tree on `Drop`. A 5 s graceful-shutdown window precedes a
SIGKILL fallback. `kill`'s stderr is silenced so a successful
graceful shutdown (which races the `kill` call and finds the group
already gone) doesn't print noise to the test runner.

### CH dest table verification: `argMax(payload, _lsn)` per id

`ReplacingMergeTree(_lsn)` keeps the latest version per `ORDER BY`
key, but `FINAL` is best-effort across parts. The drill's payload
verification uses explicit `argMax(payload, _lsn) GROUP BY id` to
match what a reader of the table would see at steady state. Row
count uses `count() FROM ... FINAL WHERE _op != 'delete'` — the
delete-as-tombstone-event model means deleted rows persist in the
table with `_op = 'delete'` and the reader filters them.

## Schema-evolution drill (phase8_add_column_replicates_pre_and_post_alter)

Second integration test in the same file, exercising `ALTER TABLE ...
ADD COLUMN` mid-stream:

- Source starts with a 2-column table.
- CH dest table + emitter mapping pre-declare the 3-column post-ALTER
  shape.
- Workload: INSERT (pre-ALTER, descriptor has attnums 1 + 2), ALTER
  TABLE ADD COLUMN c int DEFAULT 7 (catalog-only xact), INSERT
  (post-ALTER, descriptor has attnums 1 + 2 + 3), pg_switch_wal.

Pre-ALTER row lands in CH with `c = NULL` (the heap tuple physically
has no `c` value; the decoder doesn't replicate PG's read-time
"missing default" injection). Post-ALTER row carries `c = 42`. Test
asserts both directly, plus matching `count()` across source and CH.

Two emitter / harness changes the drill needed:

- **`TablePlan::build` no longer rejects mapping attnums absent from
  the catalog descriptor.** The old behaviour treated this as a hard
  config error (`source attnum N not in catalog descriptor`); under
  schema-evolution the same mapping is legitimately ahead of the
  catalog for pre-ALTER xacts. `TableEncoder::append_row` already
  emits NULL when `decoded.{new,old}.columns.get(attnum-1)` returns
  None, so the missing-column case lands as NULL on every row of the
  affected mapping column. Operators chasing a static-config typo
  still see it — the CH dest catches it if the column is non-nullable;
  otherwise it surfaces in row-count / aggregate mismatches.
- **`ChServer::drop` uses `SYSTEM SHUTDOWN`** as the primary
  graceful-exit path. The SIGTERM-to-process-group dance the existing
  drill landed with raced the CH watchdog: once the watchdog has
  forked the worker and exited (its standard daemonisation), `kill
  -TERM -<pgid>` could no longer find a live group member, the
  worker survived, and the next test's `ChServer::spawn` collided on
  the worker's still-held default ports (mysql/postgresql/grpc).
  `SYSTEM SHUTDOWN` is CH's documented graceful-exit SQL primitive
  ([docs](https://clickhouse.com/docs/sql-reference/statements/system#shutdown))
  — the watchdog + worker wind down together, `try_wait` reaps the
  immediate child, the process tree is gone. SIGKILL to the process
  group remains as a fallback.
- **`ChServer::spawn` overrides default mysql/postgresql/grpc/
  prometheus ports** to empty (`--mysql_port=`, etc.). CH's embedded
  default config binds those alongside `tcp_port`; on a host that
  has a separately-installed CH instance or a stray test daemon, the
  default ports collide and the spawned server fails before binding
  the test's TCP port.

## What didn't get done

- **DROP TABLE.** PLAN.md's Phase 8 prose names a five-statement
  workload (`CREATE TABLE`, `INSERT`, `ALTER TABLE ADD COLUMN`,
  `UPDATE`, `DROP TABLE`). The two tests here cover the first four;
  `DROP TABLE` is a no-op for CH replication (the CH dest is not
  dropped when the source table goes away — that's an operator
  decision, not a replication primitive). Worth a followup test
  asserting "DROP TABLE on source does NOT remove the CH dest" so
  the contract is pinned.

- **PG read-time default replication.** A pre-ALTER row read on
  source after `ALTER TABLE ADD COLUMN c int DEFAULT 7` shows
  `c = 7` because PG injects the missing default at read time
  (`pg_attribute.atthasmissing`). walshadow's decoder reads the
  physical tuple bytes and emits NULL for columns the tuple lacks.
  The schema-evolution test pins NULL as the expected behaviour for
  pre-ALTER rows — when a future commit wants to replicate the
  PG-equivalent semantics, the decoder needs to consult
  `pg_attribute.atthasmissing` + `attmissingval` (a binary-format
  default) at decode time. Out of scope for v1.

- **CI matrix coverage for Phase 8.** The matrix added in the
  preceding commit runs PG 16/17/18 against the suite, but the
  Phase 8 drill needs `pg_basebackup` (provided by
  `postgresql-client-<major>`, already installed) *and* the
  `clickhouse` multitool's `server` subcommand. CI installs
  `clickhouse-server` + `clickhouse-client` + `clickhouse-common-static`,
  which together provide `/usr/bin/clickhouse` — the multitool form
  the drill uses. First CI run will confirm or surface gaps; the
  drill skips silently if the binary isn't present, so an
  install-mismatch shows as "skipped" not "failed".

- **`BufferingDecoderSink` + `Emitter` audit for the
  `Allocator`-pointer-pinning class of bug.** Item 2 above fixed the
  one site that crashed; `BlockBuilder::new` takes `Allocator` by
  value and is a candidate to repeat the pattern when the emitter's
  mid-xact-flush path materialises. No issue today because the
  builder doesn't outlive its construction frame; flagging for the
  next emitter refactor.

- **`PHASE8.md` design layer.** Phase 6 carries a sibling
  `PHASE6disk.md` for the design-vs-implementation analysis. Phase 8
  is small enough to fit in this single retro; no design-layer doc.

## Live observations

Running the drill against PG 18.4 + ClickHouse 25.8.1 locally:

- `pg_basebackup` of a fresh cluster with one user table: ~150 ms,
  ~40 MiB written to shadow's data dir.
- Shadow PG bootstrap to "ready to accept connections" in standby
  mode: ~250 ms. First `pg_last_wal_replay_lsn()` non-NULL within
  ~80 ms of standby start.
- `clickhouse server` spawn to first accepted `SELECT 1`: ~700 ms
  (the dominant cost of the test).
- Daemon attach + workload + pump + verify: ~400 ms.
- End-to-end: 1.5 s wall.

`decoder=decoded=9 ins=5 hot=3 del=1` matches the workload exactly
(5 inserts; 3 update-with-payload-change recognised as HOT updates
under `REPLICA IDENTITY FULL`; 1 delete). `commit=3` reflects the
three autocommit xacts that carried heap records (the INSERT batch,
the UPDATE batch, the DELETE). The `pg_switch_wal()` xact carries no
user heap and doesn't contribute to the count.

## Files touched

```
walshadow/src/wal_stream.rs                  dispatch-order swap in flush_current + close
walshadow/src/ch_emitter.rs                  drop TablePlan::build's strict attnum check
walshadow/tests/multi_segment_filter.rs      assertion + comment for new contract
walshadow/tests/phase8_e2e.rs                new — Phase 8 + schema-evolution drills
walshadow/plans/INDEX.md                     Phase 8 entry
walshadow/plans/PHASE8.md                    new (this doc)
walshadow/clickhouse-c-rs/src/client.rs      Pin<Box<Allocator>> + two derefs
```
