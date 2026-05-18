# PHASE12 — bootstrap rows → CH emitter: three-way comparison

Background: Phase 12 introduced a bootstrap path that produces synthetic
`CommittedTuple { op: Insert, commit_ts: 0, commit_lsn: start_lsn }`
rows from BASE_BACKUP heap pages. In the V1 retro the bootstrap drain
target is `MetricsTupleObserver` — rows are counted, not shipped to
ClickHouse. The blocker is a chicken-and-egg: `Emitter::new(config,
Arc<Mutex<ShadowCatalog>>, TcpStream)` couples to `ShadowCatalog`,
which needs a running shadow PG, which needs catalog files the
bootstrap produces. PHASE12.md ("Open items §1") flagged three
candidate buffering shapes. Three subagents implemented all three in
parallel worktrees; this doc compares the results.

## Worktree pointers

| Solution | Worktree path | Branch |
|---|---|---|
| 1 — spool to disk | `.claude/worktrees/agent-abae1207583bc4183/` | `worktree-agent-abae1207583bc4183` |
| 2 — catalog adapter | `.claude/worktrees/agent-ab919fc9b72bc5e90/` | `worktree-agent-ab919fc9b72bc5e90` |
| 3 — in-mem + sync block | `.claude/worktrees/agent-abd45505bce7690fe/` | `worktree-agent-abd45505bce7690fe` |

Worktrees branched from `main` (commit `e1a0314`); Phase 12 changes
were unstaged in the parent working tree at dispatch time. Solution 2's
agent applied the Phase 12 patch on top of its branch and wired the
solution into `run_bootstrap` end-to-end. Solutions 1 and 3 left their
implementations as self-contained modules ready to drop into a
Phase-12-landed branch.

## At a glance

| Axis | Solution 1 — spool | Solution 2 — adapter | Solution 3 — buffer |
|---|---|---|---|
| Core LOC added | 416 (module) + 127 (e2e) | 179 (trait + impls) | 412 (module) + 219 (e2e) |
| Files touched outside new module | 3 (spill.rs `pub(crate)`, lib.rs, stream.rs +10) | 3 (ch_emitter.rs +11/-12, stream.rs +~70, drain_backfill +6) | 2 (lib.rs, stream.rs +16) |
| New runtime dependency | none | none | none |
| New on-disk artifact | `bootstrap.spool` | none | none |
| Memory peak | bounded (64 KiB BufWriter) | `O(tables × byte_budget)` ≈ `O(MB)` | `O(sum of source heap rows)` — unbounded |
| Extra CH connection | none (one in replay phase) | one (transitional, drained then closed) | none |
| Extra catalog source | none (rows read back into existing emitter) | yes — `CatalogMapResolver` alongside `ShadowCatalog` | none |
| Per-row hot-path cost | + fsync amortised + replay-time decode | + 1 vtable indirection | nil during buffer, normal during drain |
| Sync work in async context | none (tokio fs throughout) | none | `tokio::task::block_in_place` for `psql wait_for_replay` |
| Wire-touching refactor needed | no | yes — Emitter constructor + every callsite | no |
| New tests | 7 unit + 2 e2e | 3 unit | 6 unit + 4 e2e |
| Crash recovery | unlink stale spool at boot | nil (no state to recover) | nil (Vec lives only in process) |

## Solution 1 — spool to disk

New module `src/bootstrap_spill.rs` introduces `BootstrapSpillWriter`
(`TupleObserver` impl that appends each `CommittedTuple` to
`<spill_dir>/bootstrap.spool`) and `BootstrapSpillReader` (one-tuple-at-
a-time iterator). The binary format is shared with `src/spill.rs`'s
per-xact spill format via `pub(crate)` exposure of `encode_heap` /
`decode_heap` / framing helpers — one format, one decoder.

Operator-facing shape:

- File: `<spill_dir>/bootstrap.spool`
- Header: `WSBSPL` (6 B) + `u16 LE version = 1`
- Body: `[u8 tag = 0] [u32 LE len] [encode_heap(decoded) | i64 LE commit_ts | u64 LE commit_lsn]` per row
- Tag byte stays explicit so future record kinds (schema barriers) can land alongside without breaking older readers; unknown tag surfaces as `SpillError::Format`.

Crash recovery: `maybe_clear_stale(spill_dir)` runs at daemon boot,
unconditionally removes any leftover `bootstrap.spool`. The contract
is "every row in the spool is re-derivable by re-running BASE_BACKUP,
so stale spools are safe to discard." Refinement: gate cleanup on
`bootstrap_mode != off` AND cursor-not-past-bootstrap so a daemon
restart mid-bootstrap doesn't lose work.

Replay coupling: not yet wired. The agent built the spool module +
its `TupleObserver` impl, then noted that `run_bootstrap` doesn't
exist in the branch. Wiring shape is straightforward: replace
`MetricsTupleObserver` drain target with `BootstrapSpillWriter`; after
the WAL-side `Emitter` is built (around `bin/stream.rs:475-503`),
spawn a one-shot replay task that opens `BootstrapSpillReader`, pumps
each tuple through `emitter.route_with_retry`, calls
`drain_xact_with_retry`, unlinks the file. The replay task must
finish before the main WAL loop starts so post-attach WAL records
that ReplacingMergeTree-collapse with backfill rows see the bootstrap
rows already in CH.

Where it wins:

- Memory exposure is constant (64 KiB BufWriter). pgbench scale 1000 (~600 GB heap) flows through unchanged.
- No emitter refactor — the existing `Emitter::new(.., ShadowCatalog, ..)` stays untouched.
- Format reuse with `spill.rs` means one place to evolve tuple encoding (a future TOAST chunk decoder lands once).
- Spool is auditable — operators can checksum it, copy it for replay drills, inspect with the existing `spill` decoder.

Where it loses:

- Two reads of each tuple: PageWalkSink → spool (write), then spool → emitter (read). Decoded twice (encode_heap on write, decode_heap on read).
- Latency: bootstrap rows don't land in CH until shadow PG is up + WAL-side emitter is built. Same wall-clock as Solution 3, slower than Solution 2.
- Disk space: the spool can grow to the full source heap size before replay starts.
- One new file shape to maintain (versioned magic + format).

## Solution 2 — catalog adapter

New module `src/relation_resolver.rs` defines a one-method async trait:

```rust
pub trait RelationResolver: Send + Sync {
    fn relation_at<'a>(&'a self, rfn: RelFileNode, at_lsn: u64)
        -> Pin<Box<dyn Future<Output = Result<Arc<RelDescriptor>, CatalogError>> + Send + 'a>>;
}
```

`Mutex<ShadowCatalog>` and a new `CatalogMapResolver` (snapshot-only,
ignores `at_lsn`) both implement it. `Emitter::new` and
`EmitterConfig::connect` now take `Arc<dyn RelationResolver>` instead
of `Arc<Mutex<ShadowCatalog>>` — coercion at call sites makes the
churn tiny (existing `Arc<Mutex<ShadowCatalog>>` callers compile
unchanged through auto-coercion).

Approach choice (a static-generic vs b trait-object): the agent picked
**(b) trait-object**, citing that the daemon already type-erases the
observer chain via `Box<dyn TupleObserver>` so static-generic
propagation would dead-end. One vtable indirection per `route` call
is well below CH encoding cost.

`run_bootstrap` integration: when `--ch-config` is set, the agent
builds a transitional `Emitter` against `CatalogMapResolver` (cloned
from the seeded `CatalogMap`), drains backfill tuples through
`EmitterObserver` wrapping it, closes via `on_xact_end` /
`drain_xact_with_retry`, then drops the emitter so its CH TCP closes
cleanly. The main daemon flow downstream builds a fresh `Emitter`
against `ShadowCatalog` for WAL records, unchanged. The two emitters
share zero state on the daemon side; on the CH side they hit the same
tables under the same compression settings.

Where it wins:

- Bootstrap rows land in CH **synchronously during the pump**. No replay phase, no double-decode, no disk write. Shortest wall-clock to first row visible in CH.
- Bounded memory by construction: peak = `O(tables × TableEncoder.byte_budget)` (default 1 MiB) rather than `O(total rows)`. The streaming `INSERT … FORMAT Native` flushes mid-stream when each per-table encoder crosses its byte/row budget.
- Per-row vtable indirection is the only overhead, dominated by CH encoding cost.
- No new on-disk format, no crash-recovery state, no daemon-boot cleanup logic.
- The trait abstraction is **independently useful**: any future "alternate catalog source" (e.g. a logical-decoding-driven mirror, or a CDC-source-of-truth shape) can implement `RelationResolver` and ride the same emitter.

Where it loses:

- Touches the emitter's hot path. Every CH-emitter test recompiles. A regression in `RelationResolver` shows up as a wire-level failure, not a unit-test failure.
- Two catalog sources in flight during bootstrap (`CatalogMap` snapshot + later `ShadowCatalog`). They must agree; Phase 12's existing "DDL quiesced during bootstrap" stipulation covers it but it's one more contract to enforce.
- Transitional + production emitters share table-name + INSERT semantics on the CH side. If a table's schema changes between bootstrap end + WAL start, the second emitter's `TablePlan::build` may differ — same risk Solution 3 has.
- `at_lsn` is silently ignored by `CatalogMapResolver`. If the bootstrap somehow sees a tuple whose filenode post-dates the seed snapshot, the resolver returns the seed's view rather than failing loudly. (Failure mode is a wrong relation match, not a panic.)

## Solution 3 — in-mem buffer + sync block

New module `src/backfill_bootstrap.rs` (the agent named the module
identically to the parent's existing one; in a real landing this
collides — file/module rename required). `BufferedBackfillObserver`
parks every `CommittedTuple` in a `Vec`, tracks bytes-estimate +
tuples-count high-water marks. `drain_into_emitter(...)` is the
post-bootstrap entry point: it optionally starts shadow PG via
`Shadow::start`, calls `Shadow::wait_for_replay(bootstrap_end_lsn,
timeout)` inside `tokio::task::block_in_place` (because `wait_for_replay`
shells out to `psql`), connects `ShadowCatalog`, opens CH TCP, builds
`Emitter`, routes every buffered tuple, closes via
`drain_xact_with_retry`.

Two CLI flags added (currently dormant — no `run_bootstrap` exists in
the worktree's branch):

- `--bootstrap-autospawn-shadow` (default `false`). When `true`, the daemon owns shadow lifecycle via `Shadow::start`. When `false`, the daemon assumes an external operator (systemd, k8s) starts shadow; uses `with_transient_retry` on `ShadowCatalog::connect` until shadow appears.
- `--bootstrap-shadow-replay-timeout` (default 300 s).

Memory-pressure surface: `TUPLE_THRESHOLD_WARN = 1_000_000` rows OR
`BYTE_THRESHOLD_WARN = 256 MiB`. Either threshold flips
`BufferStats::should_warn() == true`; caller logs a warn-line. There
is **no hard cap** — Solution 3 documents that it accepts unbounded
memory and recommends 1/2 for larger sources.

Where it wins:

- Smallest delta in lines of code touching pre-existing files (just `lib.rs` + 16 lines in `stream.rs`). No emitter refactor; no on-disk format; no `spill.rs` change.
- Tightest sequencing reasoning: bootstrap finishes → shadow up → emitter built → rows drained → main loop starts. No concurrent state, no in-process catalog duplication.
- Crash recovery is trivial — the Vec dies with the process, restart starts from scratch (re-bootstraps, same as Solution 1's recovery semantics).
- Auto-spawn flag is independently useful: even without this solution, "daemon owns shadow lifecycle" is a feature users will want.

Where it loses:

- Unbounded memory cost is the wall: pgbench scale 10 (~6M rows, ~1.5 GB raw) is on the edge of acceptable; scale 100+ is not. Operators with non-trivial source databases can't use it.
- Wall-clock latency: rows don't land in CH until shadow PG has fully recovered to `end_lsn` (potentially minutes for a large catalog). Same as Solution 1, worse than Solution 2.
- `tokio::task::block_in_place` is correct here but couples the bootstrap path to multi-threaded runtime. A single-threaded runtime would deadlock.
- Threshold is warn-only; no automatic fall-back to disk spill (i.e. no graceful degradation to Solution 1).

## Recommended path

**Solution 2 (catalog adapter) is the right primary choice.**

The reasoning is mostly about the failure mode at scale. Solution 3's
unbounded memory means there exists a source-database size where the
daemon OOMs during bootstrap; Solution 1 avoids that but doubles the
encode/decode cost and adds a new on-disk format. Solution 2's
streaming-emitter peak is `O(tables × byte_budget)` and is the only
shape where the memory profile holds at pgbench scale 1000+. The
`RelationResolver` trait is also the smallest *conceptual* change: it
names a contract that was already implicit in the emitter ("I need to
resolve a filenode to a descriptor"). The same trait will be useful
for any future alternate-catalog scenario.

The emitter refactor is the only real concern, and it's mechanical:
the agent's diff is `+11/-12` in `ch_emitter.rs`. Every CH-emitter
test recompiles unchanged through `Arc<Mutex<ShadowCatalog>>` →
`Arc<dyn RelationResolver>` auto-coercion.

**When Solution 1 wins instead:** the operator profile that wants
auditable backfill state on disk — e.g. a regulated environment where
"what data did the bootstrap ship?" needs a deterministic answer
without re-running BASE_BACKUP. The spool file is that artifact. Also
the right pick if the future plan is to ship the spool between
machines (e.g. bootstrap on a beefy temp host, replay on the
operational daemon).

**When Solution 3 wins instead:** small sources (~10s of MB of heap),
single-machine drills, demo / CI configurations where memory headroom
isn't a concern and minimal code surface matters more than scale.
The auto-spawn-shadow flag from Solution 3 should land regardless,
because operators will want it.

## Hybrid worth considering

Solution 2's `RelationResolver` trait + Solution 1's spool, composed:

1. Bootstrap drains through `EmitterObserver` against
   `CatalogMapResolver` (the Solution 2 path).
2. The emitter ships to CH live — no buffer in memory or on disk.
3. The spool from Solution 1 becomes a side-channel for audit /
   recovery: written in parallel as a tee, not on the critical path.

Cost: one extra TupleObserver in the fan-out (the tee), one extra
encode per row, one disk file. Buys: live emit (Solution 2's win) +
auditable backfill artifact (Solution 1's win). The Solution 1 spool
module is ready to plug in as that tee; no further code needed
beyond a `Tee` observer wrapper that fans `on_tuple` to two
underlying observers.

Whether this hybrid is worth the disk cost is an operator-policy
question, not a technical one. Default off; flag-on when audit is
required.

## Integration plan from current `main`

The parent working tree currently has Phase 12 changes uncommitted on
top of `main` (commit `e1a0314`). Solution 2's worktree applied that
patch first, so its diff is what would land. To pick Solution 2:

1. Merge / cherry-pick the Phase 12 patch onto a `phase12` branch (or
   land it as-is and base the bootstrap-emitter work on top).
2. From the `agent-ab919fc9b72bc5e90` worktree, lift:
   - `src/relation_resolver.rs` (new file).
   - The `Emitter` / `EmitterConfig::connect` signature change in `src/ch_emitter.rs` (auto-coercion handles call sites).
   - The `on_xact_end` call in `src/backfill_bootstrap.rs::drain_backfill`.
   - The `run_bootstrap` integration in `src/bin/stream.rs` (ch_config plumbed through, transitional emitter path).
   - `pub mod relation_resolver;` in `src/lib.rs`.
3. Add a live-PG variant of `phase12_object_store_e2e` that asserts
   bootstrap rows actually land in a spawned ClickHouse server (the
   existing test asserts they reach `CollectingTupleObserver`;
   extending it to drive the transitional emitter against
   `ChServer::spawn` from `tests/phase8_e2e.rs` is mechanical).
4. Update PHASE12.md's "Open items §1" to note Solution 2 landed; flag
   the spool tee as an optional follow-up if audit is wanted.

Solution 3's `--bootstrap-autospawn-shadow` flag is worth lifting
independently — orthogonal to the bootstrap-emitter choice.
