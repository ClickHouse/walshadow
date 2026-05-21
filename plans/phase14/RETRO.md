# Phase 14 — retro

What landed vs what the plan called for, plus surprises and
follow-up debt picked up wiring eight items end-to-end. Six are
product changes; three are integration tests. Five product items
landed clean; the test surface is mostly `#[ignore]` stubs awaiting
CI fixture support

## What landed

### §1 — Read-time defaults

`RelAttr.missing_text: Option<String>` carries
`pg_attribute.attmissingval[1]::text` from the catalog fetch through
to `decode_tuple_payload`'s `idx >= natts` branch. PG's `attmissingval`
is `anyarray`, so the SQL casts to `text` and a shared
`shadow_catalog::parse_array_one_element` helper strips `{...}` +
unquotes. `heap_decoder::missing_value_for(att)` resolves text →
`ColumnValue` over the Tier 1/2 matrix; Tier 3 routes to
`PgPending` so the existing oracle path handles it at emit time

Surprise: the plan called for decoding `attmissingval` bytes via "the
`codecs.rs` array-element path against the column's type oid" — that
path doesn't exist. PG's text representation via `array_out` is the
shortest viable bridge to the column type. Numeric / Tier 3 missing
defaults stay correct because `PgPending` survives all the way to
emit time

### §2 — XLOG_HEAP2_MULTI_INSERT

`decode_heap_record` return type widened to
`SmallVec<[DecodedHeap; 1]>` (aliased `DecodedHeaps`); zero-allocation
on the single-tuple INSERT/UPDATE/DELETE path, spills to heap only
for MULTI_INSERT records carrying > 1 tuple. `decode_multi_insert`
walks `xl_heap_multi_insert (flags + ntuples)` then per-tuple
`xl_multi_insert_tuple` headers, synthesising a 5-byte
`xl_heap_header` prefix per tuple so the existing
`decode_tuple_payload` handles the body unchanged

Surprise: plan referenced an `XLH_INSERT_NO_LOGICAL` flag — that
constant doesn't exist in PG sources. The actual gate is
`XLH_INSERT_CONTAINS_NEW_TUPLE`, set whenever logical decoding is
enabled (walshadow's `wal_level=logical` floor) and the writer
included tuple data. Decoder skips records lacking this flag

Surprise: callers of `decode_heap_record` spread further than the
plan's "single iteration site in `BufferingDecoderSink::on_record`"
admitted — `decoder_sink.rs`, `xact_buffer.rs`, six unit-test
modules, and three fixture builders all consumed the
`Option<DecodedHeap>` shape. Migration was mechanical but spread
across the codebase

### §3 — TRUNCATE propagation

`HeapOp::Truncate` variant; `main_data::parse_xl_heap_truncate`
extracts `(dbId, nrelids, flags, relids)`;
`BufferingDecoderSink::handle_truncate` intercepts the info-op
before the block-ref check, gates on `wait_for_replay(source_lsn)`,
walks each relid via `ShadowCatalog::relation_by_oid`, filters to
`kind ∈ {'r', 'p'}`, and pushes one `DecodedHeap{op:Truncate}` per
relation into the xact buffer. Emitter `route` drains any in-progress
table encoder for the relation before issuing `TRUNCATE TABLE
<dest>` synchronously

Surprise: TRUNCATE is the only heap op where the WAL record carries
pg_class OIDs rather than relfilenodes — `decode_heap_record`'s
single-relation contract doesn't fit. Option (a) from the plan
(intercept pre-decode in the sink with catalog access) was the
cleaner shape

`RESTART_SEQS` flag still ignored — sequence-state replication is
deferred to v1.1 per PLAN.md gap #5

### §5 — Subxact lineage + ROLLBACK TO SAVEPOINT

`SubxactTracker` (parent + children maps) keyed on subxid; populated
from `XLOG_XACT_ASSIGNMENT` (info `0x50`) as a hint, not a gate —
PG batches first-64 subxacts without an explicit assignment record,
so the commit/abort record's subxact list is authoritative.
`parse_xact_payload` walks PG's `xinfo`-gated tails
(`xact_time → dbinfo → subxacts → relfilelocators → dropped_stats →
invals → twophase[→gid] → origin`), extracting subxacts when
`XACT_XINFO_HAS_SUBXACTS` is set. `XactBuffer::commit` /
`XactBuffer::abort` widen by a `subxids: &[u32]` slice; commit
k-way-merges the per-xid buffers in `source_lsn` ASC order before
emitting

Surprise: plan stated `XACT_XINFO_HAS_DBINFO = 1<<1`, but PG source
has `HAS_DBINFO = 1<<0` with everything shifted down one. The agent
landed the PG-source values per the plan's "pin against PG source"
note. Affects skip-walk through unread tails, not subxact extraction
itself

The tracker exists mostly as eviction-policy hint — for orphan
prevention the commit-record subxact list is the load-bearing
piece. Kept the tracker surface minimal; no gold-plating

### §6 — Apply-lag metrics

Four new gauges/counters in `metrics.rs`:
`walshadow_shadow_apply_lag_bytes` (gauge),
`walshadow_shadow_apply_lag_seconds` (gauge, `{:.3}` precision),
`walshadow_shadow_stream_active_connections` (gauge),
`walshadow_shadow_stream_dropped_connections_total` (counter). The
seconds gauge renders `+Inf` (Prom convention) when the rate
estimator can't compute a denominator. A 30-second rolling
`RateEstimator` in `bin/stream.rs` feeds it. Status-line tracing
gains `shadow_apply=<lsn>` alongside `dispatched=<lsn>`

Surprise: plan said extend `tests/phase10_ops.rs`'s scrape
assertion — that test doesn't have one. The actual scrape live test
is `tests/bin_stream_e2e.rs:318`. Wired there instead

Surprise: `dropped_total` needs guarding against double-count when
the same connection trips the slow-cutoff path twice — fixed via a
`!c.closing` gate before bumping

### §7 — kill -9 + restart test

`tests/phase14_kill_restart.rs`. Three strategies × five seeded LCG
windows = 15 cycles. `WALSHADOW_KILL_SEED` env (default `0xC11AC11A`)
seeds an inline splitmix-style LCG so CI is reproducible.
`tokio::process::Child::start_kill()` sends SIGKILL on Unix without
needing a `nix` dep. CH server stays alive across all 15 cycles;
spill dir + cursor file persist between kill + restart per the
phase 13 dual-cursor contract

Departure from plan: §"post-commit / pre-CH-ack" strategy uses the
plan's `§Risks` fallback (kill the moment
`walshadow_xacts_committed_total > 0`) rather than the original
CH-side artificial-delay shim. Same intent, simpler harness

### §8 — pgbench acceptance test

`tests/phase14_pgbench_acceptance.rs`. `pgbench -i -s 1` seeds, then
the daemon runs through `--bootstrap-mode=direct
--bootstrap-autospawn-shadow`. Workload `-T 30 -c 4 -j 2` intermixes
with one `ALTER TABLE ... ADD COLUMN c int DEFAULT 7` at +10 s and
one `CREATE INDEX CONCURRENTLY` at +20 s. Drain via `pg_switch_wal`
+ `--max-segments=1` clean-exit gate, then per-table count + sum +
item-1 c-column parity check

Departure from plan: `c` column on the CH dest is mapped
`Nullable(Int32)` not `Int32`. Bootstrap walks heap pages where
attnum=5 doesn't yet exist (ALTER fires post-bootstrap), and the
emitter writes NULL for missing-attnum mapping columns. Non-nullable
rejects bootstrap inserts. Assertion adjusted to "no row has c ≠ 7
and not NULL" + "at least one row reaches CH with c=7 via the
read-time-default path", proving item 1's end-to-end wiring

### §9 — Bootstrap CH e2e

`tests/phase14_bootstrap_direct_ch.rs` +
`tests/phase14_bootstrap_object_store_ch.rs`. Shared scaffolding in
`tests/common/bootstrap_ch_fixture.rs` (`#[path = ...]` mod include —
Cargo would otherwise build `common` as a free-standing test
binary). Both tests gate readiness on the daemon's `--metrics-bind`
endpoint binding, which by construction means bootstrap finished +
the transitional emitter's INSERTs drained synchronously

Surprise: `--bootstrap-autospawn-shadow` doesn't rewrite shadow's
`port` / `unix_socket_directories` — BASE_BACKUP ships source's
`postgresql.conf` verbatim, so the autospawn'd shadow collides with
source's port. Tests pre-seed shadow-port overrides into source's
`postgresql.auto.conf` before the daemon runs. PG honours last-wins
per key, so the override survives BASE_BACKUP into shadow's data
dir. **Operator-facing wart** — see future work

## Tests + acceptance

`cargo test --workspace --lib` — 286 walshadow unit tests pass.
Coverage includes:

- `heap_decoder::tests::missing_value_for_*` — read-time-default
  resolver across Tier 1/2/3
- `heap_decoder::tests::multi_insert_*` — 3-row decode + ntuples=0
  malformed + `CONTAINS_NEW_TUPLE` gate
- `main_data::tests::xl_heap_truncate_*` — relid array parse
- `xact_buffer::tests::subxact_tracker_*` — assign + top_for +
  forget_tree + retarget edge
- `xact_buffer::tests::parse_xact_payload_*` — xinfo-gated tail walk
- `xact_buffer::tests::abort_with_subxids_drops_each_buffer`
- `metrics::tests::rate_estimator_*` + render lines for the four
  new metrics

Integration test compile-check passes (`cargo check --workspace
--tests`), `cargo clippy --workspace --tests` clean

### Acceptance items, audited

- ✅ `cargo test --workspace --lib` green
- ✅ `walshadow_shadow_apply_lag_bytes` +
  `walshadow_shadow_apply_lag_seconds` visible on the metrics
  endpoint
- ✅ PLAN.md correctness gaps #1 (MULTI_INSERT), #3 (TRUNCATE),
  #4 (read-time defaults) — decoder paths landed
- ✅ PLAN.md correctness gap #2 (subxact) — decoder + buffer paths
  landed; tracker is a hint, commit-record subxact list is
  authoritative
- ⚠️ v1.0 acceptance §1 (pgbench, item 8) — test scaffolding ships
  as `#[ignore]`; never run against PG 16/17/18 in CI. Plan called
  for green-in-CI; we're code-complete but un-driven
- ⚠️ v1.0 acceptance §5 (kill-restart, item 7) — same posture.
  `#[ignore]`, never run end-to-end
- ⚠️ Item 9 (bootstrap-CH e2e) — both tests `#[ignore]`. Local
  loopback never exercised the assert path; only the daemon-spawn
  + metrics-gate machinery is exercised by compile

## What didn't land in Phase 14

### Integration test runs

The bulk of the gap. Seven new `tests/phase14_*.rs` files ship as
`#[ignore]` because each needs source PG + CH + (usually)
basebackup-cloned shadow. Local fixtures exist (`Shadow` from
`src/shadow.rs`, `ChServer` from common), but no test was driven
end-to-end during the agent dispatch. Acceptance items remain
unverified against a live topology

Concretely: read-time defaults' `c=7` substitution, MULTI_INSERT
fan-out under COPY, TRUNCATE downstream effect, savepoint ROLLBACK
semantics, kill-restart end-state agreement — all unit-tested on
the decoder side, none confirmed against the full daemon

### PG 16/17/18 fixture pinning

Plan §02 §"Risks" called for snapshotting MULTI_INSERT fixtures
across PG 16/17/18 majors via the existing
`tests/classify_fixture.rs` infra. Not done. Same call-out for §05's
`XACT_XINFO_HAS_SUBXACTS` layout. Cross-major drift in either
record's tail-walk semantics would surface as a silent decoder
mismatch under one specific major

### CH-side TRUNCATE semantics

v1 emits a single `TRUNCATE TABLE <dest>` per relation; no per-table
`truncate_strategy = "passthrough" | "ignore"` knob. Plan flagged
this as defer-until-asked. Stays deferred

### Bootstrap-autospawn-shadow port override

Operator-facing wart: `--bootstrap-autospawn-shadow` doesn't rewrite
shadow PG's `port` / `unix_socket_directories` / `listen_addresses`
into the basebackup'd `postgresql.conf`. The phase 14 tests work
around it by appending overrides to source's
`postgresql.auto.conf` before BASE_BACKUP; in production an operator
hits the same collision and has to know to pre-seed the override.
The fix is one-screen — the autospawn path should write these three
keys into the cloned data dir's `postgresql.auto.conf` after
basebackup completes. Filed as a v1.0 polish item

### Subxact `XACT_XINFO_HAS_INVALS` ordering verification

`parse_xact_payload` walks tails in the order PG's
`ParseCommitRecord` uses (`dbinfo → subxacts → relfilelocators →
dropped_stats → invals → twophase → origin`). The plan's documented
order had `INVALS` between `RELFILELOCATORS` and `TWOPHASE`, missing
`DROPPED_STATS`. Implementation follows PG source. Worth a fixture
test against a captured commit record from PG with all bits set to
prove the walk doesn't drift under an out-of-the-way ordering on
some major

### Item 4 — DROP TABLE propagation

Deferred to PHASE15 §6 per phase scoping. DROP TABLE rides the same
`SchemaEvent` + `DrainEntry::Catalog` channel as PHASE15's
shape-mutating DDLs; co-locating collapsed one round of plumbing

### CH-server `OnceCell` fixture

Phase 14 ships three test files that each spawn their own CH server
(~5 s startup × N). Plan §09 §"Risks" flagged hoisting into a
shared `OnceCell` if the matrix grows. With 3 phase-14 tests + 2
pre-existing phase8-style tests, total CI cost is ~25 s of
unique-CH-server boot time. Not pressing; flag if test count
doubles

### Spill format version bump

`HeapOp::Truncate` got tag `4` in the spill encoder without bumping
the spill format version field. Spill files written by an older
walshadow build that crash mid-xact and resume against a phase 14
build will misread tag `4` (older build never wrote it; resume path
falls through). Spill resume contract per
[`spill.rs`](../src/spill.rs) is "wipe on startup" (cursor file
guarantees on-disk WAL is either drained or replayable from
`decoder_lsn`), so the bump is academic, but the schema field
should rev to v2 for honesty. Filed as a v1.0 cleanup

## Surprises worth carrying forward

- **Plan accuracy under PG headers.** Two of the four PG-spec
  numbers cited in the plan turned out wrong (`XLH_INSERT_NO_LOGICAL`
  nonexistent; `XACT_XINFO` bit positions shifted). The agents that
  caught these and pinned against `~/s/postgresql/src/include/...`
  produced correct landings; future PG-spec citations in plans
  should be marked "verify against source"
- **Bootstrap-then-DDL column ordering.** The pgbench acceptance
  test forced `c Nullable(Int32)` because bootstrap walks heap
  pages where `attnum=5` doesn't exist yet. This is a general
  Phase 12 → Phase 14 interaction: any post-bootstrap `ADD COLUMN`
  produces NULL bootstrap rows + non-NULL post-WAL rows. The
  read-time-default path covers post-bootstrap WAL records, but
  bootstrap pages are pre-ALTER and emit NULL. CH-side schema
  authors must use `Nullable(T)` for any column likely added
  post-attach. Worth a PLAN.md pitfall entry
- **Agent dispatch with shared files.** Three of the six product
  items touched `heap_decoder.rs`. Serialising them into one agent
  (items 1+2+3) kept conflicts off the table. Two parallel agents
  (item 6 metrics, item 9 tests) ran clean alongside. Item 9's
  agent saw mid-flight build errors from item 1+2+3's `HeapOp::Truncate`
  variant addition; this resolved on next compile but flags the
  "background agents see partial workspace state" hazard
- **Deduplication during review pass.** Two helpers (`parse_array_one_element`
  / `walshadow_parse_array_one`) duplicated across modules with
  no good reason; the agent's rationale ("keeps the bootstrap path
  independent") didn't hold because the helper is a pure function.
  The review pass collapsed them. Likewise `http_get` / `parse_metric`
  duplicated between two test files; lifted to `common`. Future
  agent prompts should call out "if X already exists, reuse don't
  mirror"

## Future work (parked)

- Drive the seven `#[ignore]` tests in CI. Each is a one-line
  un-ignore + observation of which fixture path needs a kick
- Pin MULTI_INSERT + xl_xact_commit fixtures against PG 16/17/18
  via `tests/classify_fixture.rs`'s capture path. Surface drift as
  a unit-test diff
- Fix `--bootstrap-autospawn-shadow` to rewrite shadow's port +
  socket overrides into `postgresql.auto.conf` post-basebackup.
  Cleans up the test-harness wart and saves operator surprise
- Bump spill format version with the `HeapOp::Truncate` tag-4
  addition. Pedantic; resume contract makes it academic
- Deduplicate `tests/common/bootstrap_ch_fixture.rs::ChServer` with
  `tests/phase8_e2e.rs`'s private copy. Two callers using one
  vendored ChServer is fine; if a third lands, lift
- Operator-facing knob for TRUNCATE strategy
  (`passthrough | ignore`) once a downstream consumer asks
- DROP TABLE (PHASE15 §6) and sequence-state replication (v1.1)
  remain the visible remaining v1.0 correctness gaps
