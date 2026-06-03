# Pinned-table DDL baseline — propagate first post-start ALTER

Planning doc. Fixes: `ALTER TABLE … ADD COLUMN` on an operator-pinned
relation does not replicate to ClickHouse when the ALTER is the first
schema activity walshadow sees for that table after it starts.

## Symptom

Surfaced building docker demo (`docker/DEMO.md`). `demo.users`
is pinned in `ch-config` to `id/name/email`, gets no pgbench traffic.
Presenter runs `ALTER TABLE demo.users ADD COLUMN signup_ts timestamptz`
+ an `UPDATE`. ClickHouse never grows the column; daemon logs no
`walshadow::ch_ddl: applying sql=ALTER …` line. A prior `UPDATE
demo.users` (any row change) makes the very same ALTER propagate.

## Root cause

walshadow learns each relation's column layout from the change stream
and keeps the last-known shape in `ShadowCatalog::prev_known`
(`src/shadow_catalog.rs:389`). The schema-diff lives in
`record_descriptor` (`src/shadow_catalog.rs:611`):

- `prev_known` miss → `SchemaEvent::Added`
- `prev_known` hit + non-empty diff → `SchemaEvent::Changed`

`record_descriptor` runs from `insert` (`:918`), reached on every
descriptor fetch (`relation_by_oid` `:880`, `fetch_by_filenode` via the
decoder's first WAL touch of a relation).

`DdlApplicator` (`src/ch_ddl.rs`) maps those events to CH SQL:

- `apply_changed` (`:260`) runs `ALTER TABLE … ADD COLUMN IF NOT EXISTS`
  per added column and `mutate_mapping_for_diff` (`:372`) auto-extends
  the in-memory mapping so the emitter ships the new column. This is the
  working path — proven in the demo once a baseline exists.
- `apply_added` (`:215`) **skips operator-pinned tables**
  (`mapping_target(...).is_some()` → `stats.skipped += 1; return Ok`),
  by design: a pinned dest is operator-managed, so first sight must not
  auto-touch CH.

The gap: `prev_known` is **never seeded for pinned tables at start**.
`ShadowCatalog::seed_from_source` (`:493`) — the method that fetches
descriptors into `prev_known` — is only meaningful for `auto_create`
namespaces and, in the daemon, **is not called at all**. `stream.rs`'s
only `seed_from_source` call (`src/bin/stream.rs:573`) is the unrelated
`CatalogTracker::seed_from_source(sql_client)`, which seeds the tracker's
*filenode set* for catalog-write detection, not the catalog's descriptor
baseline. The `seed_from_source` doc even states it: "relations in other
namespaces stay undisclosed until the decoder fetches them on first WAL
touch" (`:491`).

So for a pinned table with no DML before its first ALTER, the first
descriptor fetch already carries the post-ALTER shape, `prev_known` is
empty for its oid → `Added` → `apply_added` skips → CH column never
added, and subsequent same-shape updates produce no further diff.

Auto-create tables don't hit this: `Added` on first touch runs
`CREATE TABLE` (full current shape) and records the baseline, so their
later ALTERs diff correctly.

Invalidation is not the problem: `harvest_pg_class_blocks`
(`src/catalog_tracker.rs`) coarse-fires `signal_invalidation()` even when
the pg_class tuple is `Undecoded` (PG 17 emits an HOT_UPDATE on pg_class
whose new tuple omits the relnatts-bearing prefix), so the cache does
invalidate and the descriptor is re-fetched. The missing piece is purely
the absent baseline.

## Goal / non-goals

Narrow goal: a pinned table's first post-start `ALTER ADD COLUMN` (and
RENAME / DROP) replicates to CH without any priming DML.

General goal (this revision): any relation in replication scope —
TOML-pinned today, `config_table` after `runtime_config_from_pg.md` —
produces the *same* schema-event outcome whether or not its descriptor
is cached. The in-process cache is a latency optimization over catalog
round-trips; it must never decide what CH SQL runs. See "Generalization"
below; the startup seed is one instantiation of that invariant, not the
whole of it.

Non-goals (this doc):
- Type-change replication — already rejected by `apply_changed` (logged,
  not applied); unchanged here.

## Generalization — scope and baseline, not cache warmth

The user's framing exposes the real defect: **the cache decides
semantics.** `record_descriptor` (`src/shadow_catalog.rs:611`) branches
`Added` vs `Changed` on whether `prev_known` holds the oid, so the same
relation, same source shape, same config yields different CH SQL purely
on whether an in-memory map was warm. That violates the invariant the
user wants: the cache exists to elide catalog round-trips, nothing more.

### `prev_known` wears two hats

`ShadowCatalog` keeps three maps. They look alike but answer different
questions:

- `by_filenode` / `by_oid` — the **descriptor cache**. Keyed by
  `(rfn|oid, generation)`, evictable (`max_entries`, `:923`), and
  already invariant-clean: a miss falls through to `fetch_by_filenode` /
  `fetch_by_oid` (`:938` / `:995`) and reproduces the exact descriptor a
  hit would have returned. This is the cache the user describes, and it
  already does the right thing.
- `prev_known` — the **baseline ledger**. Not a cache of anything the
  catalog can hand back: it records *the source shape as of the last
  time CH and source were in agreement*, and `compute_schema_diff` diffs
  the freshly-fetched shape against it. It is in-memory only, unbounded
  (only `emit_dropped` removes entries, `:531`), cold at every boot and
  resume, and never reconstructed on a miss.

The pinned-DDL bug is `prev_known`'s coldness leaking into the
`Added`/`Changed` decision. The narrow seed fix warms it at boot. The
general fix is to stop letting any cache's warmth pick the branch.

### Why "miss → look up the catalog" does not, by itself, fix the baseline

For the descriptor cache, "miss → fetch from catalog" is exactly right
and already implemented. For the baseline ledger it is a category error:
the catalog only knows the shape *now*. Using catalog-now as the
baseline diffs now against now → empty diff → no event. That is
boot-time drift (Gap 2) restated: a baseline you can always rebuild from
the live catalog can never tell you what changed, because it has already
moved on. The baseline is *historical state*, not a memoized catalog
read. So the user's rule holds for the descriptor cache verbatim, and
holds for the baseline only once we decide *which* authoritative store
the baseline is recovered from (below) — the source catalog is the wrong
store for it.

### Why the mapping (or CH's columns) cannot be the baseline either

Tempting shortcut: drop `prev_known`, diff the source descriptor against
the operator's `TableMapping` (or against CH's actual `system.columns`).
Both break on pinned subsets.

Take `demo.users`. Source columns `{id, name, email, internal_notes}`;
operator pins `{id, name, email}` and deliberately leaves
`internal_notes` off CH. Operator then runs `ALTER … ADD COLUMN
signup_ts`. We want `signup_ts` on CH; we must *not* add
`internal_notes`.

- Baseline = mapping `{id,name,email}` → diff vs source
  `{id,name,email,internal_notes,signup_ts}` → adds **both**
  `internal_notes` and `signup_ts`. Wrong — re-adds the excluded column.
- Baseline = CH `system.columns` `{id,name,email}` → identical result,
  identical bug.
- Baseline = source shape at agreement `{id,name,email,internal_notes}`
  → diff vs source-now → adds **only** `signup_ts`. `internal_notes`
  sits in the baseline but not the mapping, so it reads as "deliberately
  excluded," not "added since." Correct.

Only a baseline that records the *full source shape at agreement time*
separates "excluded by the operator" from "appeared after we synced."
The mapping holds the exclusion decision; the source-shape baseline
holds the agreement point. You need both, and neither is derivable from
the other or from the live catalog. This is also why Alternative A is a
footgun and why the seed must record the *descriptor*, not the mapping.

### The decision as a pure function

State the applicator's choice with no reference to cache state:

```
decide(rel, source_shape):
  if rel not in replication_scope:         # config question
      skip
  elif baseline(rel) is None:              # first agreement
      if auto_create and not on_ch(rel):   CREATE TABLE; baseline := source_shape
      else (pinned):                        baseline := source_shape; no CH work
  else:
      diff = source_shape - baseline(rel)   # added-since only
      ALTER (added/renamed/dropped); extend mapping; baseline := source_shape
```

`replication_scope` and `baseline` are the two inputs that must be
*recoverable on demand*, not "whatever the cache happens to hold."
Warmth of `by_oid` / `prev_known` changes only how fast `source_shape`
and `baseline` are produced, never which arm runs.

### Scope is a configuration question — and the Dropped path shares the bug

Whether a relation is replicated is answered by the resolved config
(TOML `tables` today, `config_table.replicate` after
`runtime_config_from_pg.md`), not by cache presence. The same
conflation hides in drop detection: `sweep_dropped` (`:565`) only polls
oids already in `prev_known`, and `emit_dropped` (`:531`) returns
`false` for an oid the catalog never fetched. A configured table dropped
at source before its descriptor was ever touched produces no `Dropped`
event — cache warmth deciding semantics again. Generalising scope to the
configured set (not the fetched set) fixes drop detection by the same
principle.

### Recoverable baseline — the spectrum

Three ways to make `baseline(rel)` recoverable rather than
cache-resident, weakest to strongest:

1. **Seed from source at boot** (the Recommended fix below). Warms
   `prev_known` for every in-scope relation before `subscribe()`, so
   within one daemon lifetime no in-scope relation is ever cold and the
   decision is cache-independent. Re-derived from the live catalog at
   each boot, so it assumes boot-shape == agreement-shape; a column
   added while the daemon was down folds silently into the baseline
   (Gap 2). Minimal machinery, fixes the reported bug, makes mid-run
   behavior fully cache-independent.
2. **Query CH for the actual state** (`system.tables` /
   `system.columns` on the dest). The honest, durable record of what CH
   holds, reconstructible across restarts with zero new persistence —
   CH *is* the store. Self-correcting (reflects manual ALTERs, partial
   applies). Closes Gap 2 for the dominant `ADD COLUMN` case for free.
   But it is not a drop-in for the column baseline: no source attnums on
   the CH side, so renames are indistinguishable from drop+add, and
   pre-overlay (implicit) exclusion is ambiguous. Best used for the
   *existence* question, not the column diff — see "Querying ClickHouse"
   below.
3. **Persist the source-shape baseline** beside the emitter checkpoint,
   keyed by oid, rewritten whenever agreement is re-reached (boot,
   `CREATE`, each applied `ALTER`). Survives restart, so a column added
   during downtime still diffs against the last *agreed* shape — closes
   Gap 2 for both pinned and auto-created tables (auto-create has the
   identical drift hole: restart → `Added` → `CREATE IF NOT EXISTS`
   no-ops → baseline silently becomes source-now). Most machinery,
   strongest guarantee for renames/drops.

Recommendation: land option 1 now (the minimum that satisfies the
invariant *within a lifetime* and fixes the demo). Then adopt the hybrid
in "Querying ClickHouse": CH existence as the table/column *creation*
discriminator (durable, cache-free, closes Gap-2-for-adds), with a
source-shape baseline (option 1's seed now, option 3's persistence
later) retained for rename/drop fidelity.

### Querying ClickHouse for the baseline

Direct answer to "can we just ask CH for the state?": yes for some of
the decision, no for the rest. CH answers two different questions with
very different fitness.

**Existence (table/column present on CH?) — CH is the *right* store.**
Today the `Added`-vs-`Changed` split keys on `prev_known` warmth; the
truthful, restart-durable version of that question is "does the dest
table exist?" (`system.tables`) and "does the column exist?"
(`system.columns`). Replacing the warmth test with a CH-existence test
is a strict improvement:

- Auto-create unifies cleanly: CH table absent → `CREATE`; present →
  reconcile columns. No `prev_known` involved, correct after restart.
- `ADD COLUMN` reconciliation closes Gap 2 for free. Source gained a
  column while the daemon was down → CH lacks it → diff source-now
  against CH-now surfaces it → `ADD COLUMN IF NOT EXISTS`. No
  persistence, no seed. Since `ADD COLUMN` is the overwhelmingly common
  real-world DDL, this is the case worth optimising for, and it is the
  one CH-as-truth nails.
- Idempotent and self-healing: `IF NOT EXISTS` / `IF EXISTS` mean a
  stale read (CH DDL is async; `system.columns` can lag a just-issued
  ALTER, more so on `Replicated`/`ON CLUSTER`) costs at most a re-issued
  no-op, never corruption.

**Column diff (what changed since agreement?) — CH is the *wrong* sole
store**, for three concrete reasons:

1. **Renames lose their anchor.** `compute_schema_diff` detects
   `RENAME COLUMN` by attnum-stable + name-changed
   (`src/shadow_catalog.rs:256`). CH stores no source attnum — only the
   mapped name. Diffing source-now against CH columns sees a rename as
   `{drop old, add new}`, and the applicator would `DROP COLUMN` the old
   CH column — **data loss**. Distinguishing rename from drop+add needs
   the *previous source shape*, which is the source-shape baseline, not
   CH.
2. **Implicit exclusion is ambiguous (pre-overlay).** A pinned subset
   leaves excluded columns absent from both the mapping and CH. "In
   source, not in CH" then means *either* "excluded" *or* "added since"
   — the footgun. This dissolves once `runtime_config_from_pg.md` makes
   exclusion explicit (`config_column.exclude`): the rule becomes "add
   if in source AND not in CH AND not excluded," and CH-as-truth becomes
   safe for adds. So CH-baseline's footgun is exactly co-extensive with
   implicit exclusion; the overlay removes it.
3. **Couples the decision to CH availability and timing.** Baseline
   resolution would now require a CH round-trip (the applicator already
   owns a CH client, so it is reachable) and inherits CH's async-DDL
   read-after-write semantics. A source-catalog seed needs only shadow
   PG, which the decision path already gates on via `wait_for_replay`.

Net: query CH for *existence* (creation discriminator + ADD
reconciliation, durable and cache-free), keep a *source-shape* baseline
for *rename/drop* fidelity. The two stores answer the two questions each
is actually authoritative for. Diffing must then run in two spaces — CH
existence checks in target-name space, the source-shape diff in PG
attnum space — and reconcile via the mapping (`src_attnum ↔ target_name`),
filtering the synthetic `_lsn` / `_xid` / `_op` / `_commit_ts` columns
out of the CH side before comparison.

### Temporal catalog — versioned descriptors vs. a single agreed point

Could the catalog become a time-series store so we query "shape as of
LSN L" directly? Two targets, very different difficulty:

- **Shadow *PG* time-travel — infeasible.** Shadow is a physical
  standby; its catalog is a moving pointer at the replay LSN. Arbitrary
  past-LSN whole-catalog snapshots need all old tuple versions retained
  against MVCC vacuum/HOT-prune — the PG-time-travel problem verbatim,
  and worse on a standby where apply/GC isn't ours to stall. PG has no
  built-in time travel. Don't.
- **walshadow *catalog layer* temporal — tractable.** We feed the WAL
  and already emit one event per shape change, so a versioned descriptor
  store is a slowly-changing-dimension table —
  `(oid, valid_from_lsn, valid_to_lsn, descriptor)` — not an MVCC engine.
  Schema versions are sparse. Minimal in-memory form is one line:
  `prev_known: HashMap<Oid, BTreeMap<Lsn, Arc<RelDescriptor>>>` bounded
  to the last K versions.

The catch: a temporal catalog alone does **not** yield the baseline. It
answers "shape as of LSN L"; the baseline is "shape at the last point CH
and source *agreed*," and the agreement LSN is the applicator's
per-relation applied-up-to-DDL high-water mark, not a point on the
source timeline. You must persist *that LSN* durably regardless — and
once persisting per-relation state, persisting the descriptor itself
(option 3) is simpler and equivalent for the baseline. Version history
earns its keep only with a second consumer of past state.

That second consumer would be race-free heap decode under concurrent DDL
(decode each tuple with the shape in effect at *its* LSN). Today that is
mostly handled already by PG's forward-compatible tuple format walshadow
relies on — `attmissingval`/`getmissingattr` for ADD, retained dropped
slots, attnum-positional decode — within the supported-DDL set. The
justification strengthens only if type-change / column-reorder support
lands, where positional decode against a raced-ahead catalog misreads.

Strongest form if pursued: an append-only **schema-history table**
(`oid, lsn, full source descriptor incl. attnums`) in CH / shadow-PG /
spill. Carrying source attnums fixes the rename gap raw `system.columns`
cannot (see "Querying ClickHouse"), and closes Gap 2 across restart.
Baseline = latest version `≤ applied_lsn`. This is option 3 generalised
from "latest agreed shape" to "full history" — marginal extra cost,
justified only when decode-correctness or audit needs the history.

Verdict: do not time-travel PG; if temporal is wanted, build it in the
walshadow layer as bounded version history — but for *this* baseline
problem option 3 (single persisted agreed descriptor) is the lighter
equivalent.

### Integration with `runtime_config_from_pg.md`

Under the overlay, `replication_scope` is the resolver's table set, not
`cfg.tables`. Two consequences for this work:

- Seed/recover the baseline by iterating the *resolved* scope, so a
  table configured via `config_table` is seeded identically to a
  TOML-pinned one.
- A scope addition at runtime (`config_table.replicate` flips true
  mid-stream) is the same event as boot for that one relation: the
  resolver must synchronously establish its baseline from source at the
  config row's commit LSN, *before* the next descriptor fetch for it, so
  the first post-opt-in `ALTER` diffs against the opt-in shape rather
  than tripping the cold-`prev_known` → `Added` path. This is precisely
  "a cache miss looks the data up," applied to the baseline ledger and
  driven by the config event instead of by boot.

## Recommended fix — seed the schema-diff baseline at startup

This is option 1 of the spectrum above: the boot-time instantiation of
the invariant. It is necessary (it makes mid-run behavior
cache-independent and fixes the demo) but not sufficient on its own for
cross-restart consistency — see "Querying ClickHouse" and option 3.

Record the current descriptor for every pinned (mapped) relation into
`prev_known` before the daemon subscribes to schema events. Then the
first post-start ALTER fetches the evolved descriptor, diffs it against
the seeded baseline, and emits `Changed` → `apply_changed` runs the CH
ALTER + extends the mapping. No change to `apply_added` semantics; no CH
work at boot; the proven `Changed` path does the rest.

This is what the user intuited: the catalog already holds the columns
(`fetch_attributes(oid)` returns the full list); we just need to read
them once at start as the baseline.

### Steps

1. **`ShadowCatalog::seed_baseline(&mut self, qualified_names: &[String])
   -> Result<usize>`** (`src/shadow_catalog.rs`):
   - For each `qualified_name` (`"namespace.relname"`), resolve oid via
     `to_regclass($1)` (same resolver preflight uses,
     `src/preflight.rs:171`), skip when it resolves to NULL (preflight
     already guarantees mapped rels exist, so this is just defensive).
   - Skip oids already in `prev_known` (idempotent across resume).
   - Call `relation_by_oid(oid)` (`:880`) — flows through `insert` →
     `record_descriptor`, populating `prev_known` (and `by_oid` /
     `by_filenode`). No event leaks because `event_tx` is `None` until
     `subscribe()` (`send_event` `:603` is a no-op pre-subscribe).
   - Return count seeded; log at info.

   Reuse the existing fetch/record path; no new SQL beyond the
   name→oid resolve.

2. **Call it in `src/bin/stream.rs`** after the catalog is connected and
   `initial_ch_config` is parsed, and **before** `subscribe()`
   (`:818`). Source the names from the pinned mapping:

   ```rust
   if let Some(cfg) = initial_ch_config.as_ref() {
       let names: Vec<String> = cfg.tables.keys().cloned().collect();
       let seeded = catalog.lock().await.seed_baseline(&names).await
           .context("seed schema-diff baseline for mapped relations")?;
       tracing::info!(target: "walshadow", seeded,
           "seeded schema-diff baseline for mapped relations");
   }
   ```

   `cfg.tables.keys()` is exactly the pinned set (auto-create tables
   aren't in `tables` until discovered, and their `Added`→`CREATE` path
   already records a baseline on first touch, so they need no seeding).

### Why this is correct

- At boot (post-bootstrap or `--start-lsn` resume) shadow PG holds the
  catalog at the same shape the pinned mapping + CH dest were built for,
  so the seeded baseline matches CH. The first ALTER after boot is a true
  delta against that baseline.
- Placement before `subscribe()` guarantees the seed emits nothing —
  only `prev_known` is warmed. Zero behavior change at boot beyond
  memory.
- Any pre-ALTER same-shape fetch (if the table did get DML) re-records
  an identical descriptor → empty diff → no event; baseline unchanged.
- Blast radius: one method + one call site, both additive. The hot path
  (`apply_changed`) is unchanged and already exercised.

### Edge cases

- Resume without bootstrap: shadow is a live standby; `seed_baseline`
  fetches its current catalog as the baseline. Future diffs are relative
  to that — correct.
- Mapped rel missing at seed time: shouldn't happen (preflight), but
  `seed_baseline` skips NULL `to_regclass` rather than erroring.
- Seed-then-immediate-ALTER race: seed runs synchronously before
  `START_REPLICATION` records flow, so the baseline is in place before
  any WAL is decoded.

## Alternatives considered

### A. Reconcile inside `apply_added` for pinned tables

On `Added` for a pinned table, instead of skipping, diff the descriptor
against the pinned mapping and run `ADD COLUMN IF NOT EXISTS` for each
descriptor column missing from the mapping, then extend the mapping
(mirroring `mutate_mapping_for_diff`).

- Pro: also fixes boot-time drift (Gap 2) since first sight reconciles
  against the live source shape.
- Con: changes the deliberate "pinned = operator-managed, don't touch CH
  on first sight" contract. A pinned table with intentionally-unmapped
  source columns would get them auto-added to CH at first sight —
  surprising. Duplicates the Changed-path add/extend logic in the Added
  path. More surface, more semantics churn.
- Verdict: heavier and more surprising than baseline-seed. Viable as the
  *complement* for Gap 2 if that case matters, gated behind an explicit
  knob (e.g. `reconcile_pinned_on_start`).

### B. Hand bootstrap descriptors straight into `prev_known`

The bootstrap emitter already builds descriptors for mapped tables; pass
them into the streaming `ShadowCatalog`'s `prev_known` instead of a fresh
shadow query.

- Pro: no extra query.
- Con: bootstrap and the streaming catalog are separate objects; plumbing
  the descriptors across is more code than a single post-connect
  `seed_baseline` query, for no correctness gain (shadow already holds
  the bootstrap shape at seed time). Not worth it.

## Deferred: boot-time drift (Gap 2)

If a column is added to source while walshadow is entirely down, at next
boot the seeded baseline equals the already-evolved source shape, so no
future diff fires and CH stays behind. Baseline-seed (option 1) does not
fix this by design (it treats the boot shape as the agreed baseline);
auto-create shares the hole (restart → `Added` → `CREATE IF NOT EXISTS`
no-ops → baseline silently becomes source-now). The principled closes
are in "Recoverable baseline": querying CH for existence closes it for
the `ADD COLUMN` case with no persistence (see "Querying ClickHouse"),
and persisting the source-shape baseline (option 3) closes it for
renames/drops too. Operators recover in the interim by SIGHUP'ing an
updated mapping. Track here; out of scope for the primary fix.

## Testing

- Unit (`src/shadow_catalog.rs` tests): `seed_baseline` populates
  `prev_known`; a subsequent fetch of an evolved descriptor for the same
  oid emits `Changed` (added column), not `Added`. Mirror the existing
  `schema_diff_detects_added_columns` test (`:1454`).
- Integration (new `tests/schema_evolution_pinned.rs`, CH+PG gated):
  bootstrap a pinned table, do **no** DML, `ALTER TABLE … ADD COLUMN`,
  drive one `UPDATE`, drain, assert the CH dest grew the column
  (`system.columns`) and a post-ALTER value lands. This is precisely the
  docker-demo scenario `pgbench_acceptance` does not cover —
  `pgbench_acceptance` pre-creates `c` on CH and pins it, so it exercises
  the decoder's read-time default, never the CH-side ALTER.
- Invariant (the generalization): the schema-event outcome is identical
  warm vs cold. Drive the same pinned-table `ALTER` twice — once with
  `prev_known` seeded, once with it forcibly cleared between fetch and
  diff — and assert the emitted event and resulting CH SQL match. A
  passing test here is the executable form of "cache must not decide
  semantics."
- Pinned-subset guard: pin a strict subset of a wider source table, run
  `ALTER ADD COLUMN newcol`, assert CH gains `newcol` and **not** the
  deliberately-excluded column. This is the regression test for the
  baseline-source footgun; it must pass under whichever store the diff
  uses.
- CH-existence path (if adopted): with the dest table dropped on CH,
  assert the first descriptor surfaces as a `CREATE`; with a column
  manually dropped on CH, assert reconciliation re-adds it (self-heal).
  Rename regression: `RENAME COLUMN` must emit a CH `RENAME`, never a
  `DROP`+`ADD` — the test that pins why CH-only cannot own the column
  diff.

## Follow-ups once landed

- `docker/DEMO.md` step 4: drop the "warm `demo.users` first" requirement;
  the row-change beat stays as a CDC demo but is no longer load-bearing.
- Update memory `reference_pinned_table_ddl_baseline` to "fixed".
- `plans/emitter.md` / PHASE15 notes: document baseline seeding as part
  of the schema-event contract.
