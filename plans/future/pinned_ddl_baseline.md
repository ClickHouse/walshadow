# Pinned-table DDL baseline — durable coherency

Design doc for schema-event coherency on operator-pinned and
`config_table`-scoped relations across restart, drops, and downtime.
Within one daemon lifetime the schema-diff baseline (`prev_known`) is
warm for the whole resolved scope — `ShadowCatalog::seed_baseline` at
boot for TOML-pinned rels, the per-table opt-in dispatch for
`config_table` rels (`plans/emitter.md` "Baseline seeding",
`plans/config.md`). Everything below extends the general invariant —
**an in-process cache must never decide which CH SQL runs** — past one
lifetime: across restart, to drops during downtime, and to durable
rename/drop fidelity.

Non-goal: type-change replication (rejected by `apply_changed`, logged
not applied; unchanged here).

## Open

1. **Boot-time drift (Gap 2).** A column added to source while walshadow
   is fully down folds silently into the next boot's seeded baseline, so
   no diff ever fires and CH stays a column behind. Auto-create and the
   opt-in boot re-seed share the hole (`CREATE IF NOT EXISTS` no-ops on
   restart → baseline becomes source-now). See "Boot-time drift (Gap 2)".
2. **Query CH for existence (the hybrid).** Replace the `Added`-vs-
   `Changed` warmth test with a CH-existence test (`system.tables` /
   `system.columns`): closes Gap 2 for the dominant `ADD COLUMN` case
   with zero persistence, restart-durable. CH is authoritative for
   *existence* only — keep a source-shape baseline for the column diff.
   See "Querying ClickHouse for the baseline".
3. **Persist the source-shape baseline (option 3).** Durable per-oid
   agreed-shape record beside the emitter checkpoint, rewritten at each
   agreement (boot / `CREATE` / applied `ALTER`). Closes Gap 2 for
   renames and drops across restart — the cases CH-existence cannot own.
   See "Recoverable baseline — the spectrum" (option 3).
4. **Drop detection across downtime.** A configured relation dropped at
   source while the daemon is down never enters `prev_known` — the boot
   seed skips a NULL `to_regclass`, the opt-in seed parks the row as a
   forward-declaration — so no `Dropped` event fires and the CH table
   lingers. Generalise sweep scope to the *configured* set, not the
   seeded set. See "Scope is a configuration question".
5. **Opt-in mapping vs republish.** `mutate_mapping_for_diff` extends
   only the live `MappingHandle` after an applied `ALTER`; the
   resolver's opt-in mapping keeps its materialize-time shape, so the
   next republish (SIGHUP, any config event) reverts the extension and
   post-ALTER columns stop routing. Extensions must land where the
   resolver merges from.
6. **Tests.** Warm-vs-cold *invariant* equivalence, a
   `RENAME COLUMN` regression (CH `RENAME`, never `DROP`+`ADD`),
   `DROP COLUMN` coverage, and the CH-existence path. See "Tests".
7. **Temporal catalog (optional).** Bounded version history in the
   walshadow layer; only earns its keep with a second consumer
   (race-free decode under type-change / column-reorder support). Do not
   time-travel shadow PG. See "Temporal catalog".

## The invariant — scope and baseline, not cache warmth

The core defect: **the cache decides
semantics.** `record_descriptor` (`src/shadow_catalog.rs`) branches
`Added` vs `Changed` on whether `prev_known` holds the oid, so the same
relation, same source shape, same config yields different CH SQL purely
on whether an in-memory map was warm. The cache exists to elide catalog
round-trips, nothing more.

### `prev_known` wears two hats

`ShadowCatalog` keeps three maps. They look alike but answer different
questions:

- `by_filenode` / `by_oid` — the **descriptor cache**. Keyed by
  `(rfn|oid, generation)`, evictable (`max_entries`), and already
  invariant-clean: a miss falls through to `fetch_by_filenode` /
  `fetch_by_oid` and reproduces the exact descriptor a hit would have
  returned. Already does the right thing.
- `prev_known` — the **baseline ledger**. Not a cache of anything the
  catalog can hand back: it records *the source shape as of the last
  time CH and source were in agreement*, and `compute_schema_diff` diffs
  the freshly-fetched shape against it. In-memory only, unbounded (only
  `emit_dropped` removes entries), cold at every boot and resume, never
  reconstructed on a miss.

Seeding warms the ledger for the resolved scope. The general fix is to
stop letting any cache's warmth pick the branch — across restart too.

### Why "miss → look up the catalog" does not, by itself, fix the baseline

For the descriptor cache, "miss → fetch from catalog" is exactly right
and already implemented. For the baseline ledger it is a category error:
the catalog only knows the shape *now*. Using catalog-now as the
baseline diffs now against now → empty diff → no event. That is
boot-time drift (Gap 2) restated: a baseline you can always rebuild from
the live catalog can never tell you what changed, because it has already
moved on. The baseline is *historical state*, not a memoized catalog
read. So the cache rule holds for the descriptor cache verbatim, and
holds for the baseline only once we decide *which* authoritative store
the baseline is recovered from — the source catalog is the wrong store.

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
The mapping holds the exclusion decision; the source-shape baseline holds
the agreement point. You need both, and neither is derivable from the
other or from the live catalog. This is why seeding records the
*descriptor*, not the mapping.

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

Whether a relation is replicated is answered by the resolved config, not
by cache presence. Within one lifetime seeding holds the two equal:
every relation in the resolved scope enters `prev_known` at boot or at
opt-in, so `sweep_dropped`'s poll set *is* the configured set. Across
downtime they diverge: `sweep_dropped` only polls oids already in
`prev_known`, and a configured relation dropped while the daemon was
down is never seeded, so no `Dropped` fires — cache warmth deciding
semantics again. Generalising the sweep to the configured set (the
config keys carry the qualified names a `Dropped` event needs) fixes
drop detection by the same principle.

### Recoverable baseline — the spectrum

Three ways to make `baseline(rel)` recoverable rather than
cache-resident, weakest to strongest:

1. **Seed from source** (`seed_baseline`, the opt-in dispatch). Warms
   `prev_known` for every in-scope relation at boot or at opt-in, so
   within one daemon lifetime no in-scope relation is ever cold and the
   decision is cache-independent. Re-derived from the live catalog, so it
   assumes seed-shape == agreement-shape; a column added while the daemon
   was down folds silently into the baseline (Gap 2).
2. **Query CH for the actual state** (`system.tables` / `system.columns`
   on the dest). The honest, durable record of what CH holds,
   reconstructible across restarts with zero new persistence — CH *is*
   the store. Self-correcting (reflects manual ALTERs, partial applies).
   Closes Gap 2 for the dominant `ADD COLUMN` case for free. But not a
   drop-in for the column baseline: no source attnums on the CH side, so
   renames are indistinguishable from drop+add, and pre-overlay (implicit)
   exclusion is ambiguous. Best used for the *existence* question, not the
   column diff — see "Querying ClickHouse" below.
3. **Persist the source-shape baseline** beside the emitter checkpoint,
   keyed by oid, rewritten whenever agreement is re-reached (boot,
   `CREATE`, each applied `ALTER`). Survives restart, so a column added
   during downtime still diffs against the last *agreed* shape — closes
   Gap 2 for both pinned and auto-created tables (auto-create has the
   identical drift hole: restart → `Added` → `CREATE IF NOT EXISTS`
   no-ops → baseline silently becomes source-now). Most machinery,
   strongest guarantee for renames/drops.

The hybrid in "Querying ClickHouse": CH
existence as the table/column *creation* discriminator (durable,
cache-free, closes Gap-2-for-adds), with a source-shape baseline (the
seed, option 3's persistence for durability) retained for
rename/drop fidelity.

### Querying ClickHouse for the baseline

"Can we just ask CH for the state?" — yes for some of the decision, no
for the rest. CH answers two different questions with very different
fitness.

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
  real-world DDL, this is the case worth optimising for, and the one
  CH-as-truth nails.
- Idempotent and self-healing: `IF NOT EXISTS` / `IF EXISTS` mean a stale
  read (CH DDL is async; `system.columns` can lag a just-issued ALTER,
  more so on `Replicated`/`ON CLUSTER`) costs at most a re-issued no-op,
  never corruption.

**Column diff (what changed since agreement?) — CH is the *wrong* sole
store**, for three concrete reasons:

1. **Renames lose their anchor.** `compute_schema_diff` detects
   `RENAME COLUMN` by attnum-stable + name-changed. CH stores no source
   attnum — only the mapped name. Diffing source-now against CH columns
   sees a rename as `{drop old, add new}`, and the applicator would
   `DROP COLUMN` the old CH column — **data loss**. Distinguishing rename
   from drop+add needs the *previous source shape*, i.e. the source-shape
   baseline, not CH.
2. **Implicit exclusion is ambiguous — for TOML-pinned subsets.** A
   pinned subset leaves excluded columns absent from both the mapping and
   CH. "In source, not in CH" then means *either* "excluded" *or* "added
   since" — the footgun. `config_table` opt-ins carry no implicit
   exclusion (the mapping derives from the full descriptor;
   `config_column` holds type overrides only), so CH-existence
   reconciliation is already safe on that scope; TOML-pinned subsets keep
   the footgun until exclusion is explicit (`config_column.exclude`,
   `runtime_config_from_pg.md`), at which point the rule becomes "add if
   in source AND not in CH AND not excluded" and CH-as-truth is safe
   everywhere.
3. **Couples the decision to CH availability and timing.** Baseline
   resolution would now require a CH round-trip (the applicator already
   owns a CH client, so it is reachable) and inherits CH's async-DDL
   read-after-write semantics. A source-catalog seed needs only shadow
   PG, which the decision path already gates on via `wait_for_replay`.

Net: query CH for *existence* (creation discriminator + ADD
reconciliation, durable and cache-free), keep a *source-shape* baseline
for *rename/drop* fidelity. Diffing then runs in two spaces — CH
existence checks in target-name space, the source-shape diff in PG attnum
space — reconciled via the mapping (`src_attnum ↔ target_name`), filtering
the synthetic `_lsn` / `_xid` / `_commit_ts` / `_is_deleted` columns out of the CH
side before comparison.

### Boot-time drift (Gap 2)

If a column is added to source while walshadow is entirely down, at next
boot the seeded baseline equals the already-evolved source shape, so no
future diff fires and CH stays behind. Seeding does not fix this by
design (it treats the seed shape as the agreed baseline); auto-create
and the opt-in boot re-seed share the hole (restart →
`CREATE IF NOT EXISTS` no-ops → baseline silently becomes source-now).
The principled closes are above: querying CH for existence closes it for
`ADD COLUMN` with no persistence, and persisting the source-shape
baseline (option 3) closes it for renames/drops too. Operators recover
without a durable baseline by SIGHUP'ing an updated mapping.

A narrower complement, absent a durable baseline:
reconcile inside `apply_added` for pinned tables — on `Added`, instead of
skipping, diff the descriptor against the pinned mapping and run
`ADD COLUMN IF NOT EXISTS` for each descriptor column missing from the
mapping, then extend the mapping. This fixes first-sight drift but changes
the "pinned = operator-managed, don't touch CH on first sight" contract
(intentionally-unmapped source columns would auto-add to CH), so gate it
behind an explicit knob (e.g. `reconcile_pinned_on_start`). Heavier and
more surprising than the durable stores above; treat as a fallback, not
the plan.

### Temporal catalog — versioned descriptors vs. a single agreed point

Could the catalog become a time-series store so we query "shape as of
LSN L" directly? Two targets, very different difficulty:

- **Shadow *PG* time-travel — infeasible.** Shadow is a physical standby;
  its catalog is a moving pointer at the replay LSN. Arbitrary past-LSN
  whole-catalog snapshots need all old tuple versions retained against
  MVCC vacuum/HOT-prune — the PG-time-travel problem verbatim, and worse
  on a standby where apply/GC isn't ours to stall. PG has no built-in
  time travel. Don't.
- **walshadow *catalog layer* temporal — tractable.** We feed the WAL and
  already emit one event per shape change, so a versioned descriptor store
  is a slowly-changing-dimension table —
  `(oid, valid_from_lsn, valid_to_lsn, descriptor)` — not an MVCC engine.
  Schema versions are sparse. Minimal in-memory form is one line:
  `prev_known: HashMap<Oid, BTreeMap<Lsn, Arc<RelDescriptor>>>` bounded to
  the last K versions.

The catch: a temporal catalog alone does **not** yield the baseline. It
answers "shape as of LSN L"; the baseline is "shape at the last point CH
and source *agreed*," and the agreement LSN is the applicator's
per-relation applied-up-to-DDL high-water mark, not a point on the source
timeline. You must persist *that LSN* durably regardless — and once
persisting per-relation state, persisting the descriptor itself (option 3)
is simpler and equivalent for the baseline. Version history earns its keep
only with a second consumer of past state.

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
cannot, and closes Gap 2 across restart. Baseline = latest version
`≤ applied_lsn`. This is option 3 generalised from "latest agreed shape"
to "full history" — marginal extra cost, justified only when
decode-correctness or audit needs the history.

Verdict: do not time-travel PG; if temporal is wanted, build it in the
walshadow layer as bounded version history — but for *this* baseline
problem option 3 (single persisted agreed descriptor) is the lighter
equivalent.

## Tests

- **Warm-vs-cold invariant.** Drive the same pinned-table `ALTER` twice —
  once with `prev_known` seeded, once with it forcibly cleared between
  fetch and diff — and assert the emitted event and resulting CH SQL
  match. The executable form of "cache must not decide semantics."
- **Rename regression.** `RENAME COLUMN` must emit a CH `RENAME`, never a
  `DROP`+`ADD` — the test that pins why CH-only cannot own the column
  diff.
- **`DROP COLUMN` coverage.**
- **CH-existence path** (once adopted): with the dest table dropped on CH,
  assert the first descriptor surfaces as a `CREATE`; with a column
  manually dropped on CH, assert reconciliation re-adds it (self-heal).
