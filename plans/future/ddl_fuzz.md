# DDL/DML semantic fuzzing

Build confidence that legal PostgreSQL schema and row transitions either
converge in ClickHouse or stop with an explicit, pre-side-effect rejection.
Generate typed transaction programs, run them through real PostgreSQL WAL and
walshadow, then compare source and ClickHouse state. Complement, do not replace,
byte-oriented parser fuzzing in [FUZZ.md](FUZZ.md)

Primary bug class is semantic success: PostgreSQL commits, walshadow reaches an
ack, but ClickHouse holds wrong schema or rows. Panic freedom and parser safety
cannot detect it

## Scope and contract

Exercise permanent tables first. Expand across unlogged tables, partitions,
tablespaces, prepared transactions, restarts, and concurrent sessions after
single-table oracle stabilizes

Classify every generated program before execution:

- `Converge` — source and ClickHouse logical row sets plus supported schema
  projection must match after emitter ack
- `RejectBeforeEffects` — unsupported transition must stop before any part of
  source transaction reaches ClickHouse
- `Policy` — configured behavior intentionally differs, for example DROP under
  `retain`; compare against policy-specific model instead of source
- `KnownGap` — minimized reproducer kept runnable and reported, never counted as
  successful confidence

Do not accept WARN plus continued ingestion as rejection. Do not accept an
unmapped-row counter as safe discard for a `Converge` case. Treat ack without
state equivalence as failure

Compare final committed state. ClickHouse cannot expose a PostgreSQL transaction
atomically while walshadow executes separate INSERT / ALTER / TRUNCATE queries;
intermediate reader visibility stays outside this plan. Restart campaigns still
check convergence from every generated execution cut

## Known transition corpus

Land deterministic seeds before random campaigns. This separates known product
limits from new interactions and gives shrinker stable endpoints

| transition | expected current outcome | contract decision needed |
|---|---|---|
| `ALTER TABLE s.t RENAME TO u; INSERT INTO s.u ...` | descriptor name changes without `SchemaEvent`; mapping remains under `s.t`, trailing row routes unmapped | move source mapping key and decide destination rename policy, or reject |
| `ALTER TABLE s.t SET SCHEMA u; UPDATE u.t ...` | same relation-name gap, namespace mapping may also change destination database | migrate route and destination, or reject |
| `ALTER SCHEMA s RENAME TO u` followed by DML | capture-all refreshes descriptors but emits no relation events | emit relation moves, or reject affected mappings |
| `ALTER COLUMN v TYPE int8` from `int4`, then insert above `i32::MAX` | CH type and mapping remain `Int32`; apply only warns for `type_changes` | migrate type before rows, or reject whole xact |
| `ALTER COLUMN v TYPE varchar(40)` from `varchar(10)` | both shapes map to CH `String`; data usually converges despite warning | classify representation-preserving transition explicitly |
| `ALTER COLUMN v TYPE text` from `varchar` with post-DDL DML | pending timeline can decode row, CH representation stays `String` | preserve as supported same-target transition |
| `ALTER COLUMN v DROP NOT NULL; INSERT (..., NULL)` | CH column stays non-nullable; emitter writes type default for NULL | alter nullability, or reject before row apply |
| `ALTER COLUMN v SET NOT NULL` | CH column stays nullable | decide whether weaker CH schema is supported projection |
| switch replica identity from PK `id` to unique `email`, then DELETE | CH `ORDER BY id` stays fixed; old tuple may carry only `email`, tombstone defaults `id` | rebuild destination key, retain immutable target key, or reject |
| drop PK constraint, drop its column | CH still uses column in `ORDER BY`; CH DROP COLUMN fails after any earlier xact effects | preflight whole plan against target key |
| add PK to table created keyless | CH retains `ORDER BY (_lsn)`; updates do not collapse by new PK | rebuild key or keep case unsupported |
| `DROP TABLE; CREATE TABLE` with same name under each drop strategy | `retain` / `warn` preserve old rows and `CREATE IF NOT EXISTS` no-ops; `drop` should round-trip | encode policy-specific lifecycle oracle |
| `DROP TABLE s.t CASCADE` with a dependent view, FK child or partition child | basic path emits the drop via `SchemaEvent` + `DrainEntry::Catalog`; dependent relations get no event of their own | decide whether cascaded relations follow the drop strategy or are left to drift |
| `DROP TABLE s.t RESTRICT` refused by source, then DML on `s.t` | source rejects, so no WAL and no event; mapping must stay intact | assert a refused DDL leaves no destination effect |
| `CREATE UNLOGGED TABLE; INSERT ...` | catalog may create CH table; user DML has no durable WAL | reject mapping or mark no-row policy explicitly |
| `ALTER TABLE ... SET UNLOGGED`, mutate, `SET LOGGED` | unlogged interval disappears; stale CH rows can survive conversion | reject persistence transition |
| attach populated partition, then write through parent | heap WAL names leaf; pinned parent target receives no fan-in and attach emits no row backfill | define partition routing/backfill semantics |
| more than pending-capture boundary cap around physical in-place drift | timeline degrades; affected rows fail ambiguity fence | preserve fail-closed behavior, assert no side effects |
| `PREPARE TRANSACTION`, restart, `COMMIT PREPARED` | process-local xact state is lost | expected `KnownGap` until [two_phase_commit.md](two_phase_commit.md) lands |
| create or move into non-default tablespace, then DML | shadow path materialization can fail; bootstrap skips source rows | expected `KnownGap` until [TABLESPACES.md](TABLESPACES.md) lands |

Also seed supported controls:

- CREATE + INSERT / multi-VALUES / COPY in one transaction
- ADD nullable column between row writes
- ADD column with fast default, including toasted and Tier 3 defaults
- RENAME COLUMN between UPDATEs
- DROP COLUMN with DML before and after, excluding CH sorting key
- TRUNCATE between inserts, including toasted values
- CREATE + ALTER + COPY with multiple pending descriptor slots
- top-level abort, savepoint rollback, savepoint release, committed subxact
- benign typmod / storage drift with post-DDL DML
- rewrite via `VACUUM FULL`, `CLUSTER`, and representation-preserving ALTER
- same programs across daemon restart at each execution barrier

## Program IR

Generate operations from typed state, never random SQL bytes. Stable identities
must survive names and object generations changing

```rust
struct Program {
    setup: Setup,
    transactions: Vec<Transaction>,
}

struct Transaction {
    actions: Vec<Action>,
    finish: Finish,
}

enum Finish {
    Commit,
    Rollback,
    Prepare { gid: Gid, resolve: PreparedResolution },
}

enum Action {
    Insert { rel: RelId, row: Row },
    InsertMany { rel: RelId, rows: Vec<Row> },
    Copy { rel: RelId, rows: Vec<Row> },
    Update { rel: RelId, key: Key, changes: Vec<Assignment> },
    Delete { rel: RelId, key: Key },

    CreateTable { rel: RelId, spec: TableSpec },
    DropTable { rel: RelId },
    RenameTable { rel: RelId, name: Name },
    MoveSchema { rel: RelId, schema: SchemaId },
    SetPersistence { rel: RelId, persistence: Persistence },
    Truncate { rels: Vec<RelId>, cascade: bool },

    AddColumn { rel: RelId, column: ColumnSpec },
    DropColumn { rel: RelId, column: ColumnId },
    RenameColumn { rel: RelId, column: ColumnId, name: Name },
    AlterType { rel: RelId, column: ColumnId, ty: PgType },
    SetNullability { rel: RelId, column: ColumnId, nullable: bool },
    SetDefault { rel: RelId, column: ColumnId, value: Option<Value> },
    SetStorage { rel: RelId, column: ColumnId, storage: Storage },

    AddUniqueIndex { rel: RelId, index: IndexId, columns: Vec<ColumnId> },
    DropIndex { index: IndexId },
    SetReplicaIdentity { rel: RelId, identity: Identity },
    AddPrimaryKey { rel: RelId, columns: Vec<ColumnId> },
    DropPrimaryKey { rel: RelId },

    Savepoint { id: SavepointId },
    RollbackTo { id: SavepointId },
    Release { id: SavepointId },
}
```

Model state per relation:

- stable `RelId`, current PostgreSQL oid/generation, schema, and name
- current `relkind`, persistence, partition parent / child relation
- stable `ColumnId`, current attnum, name, type, typmod, nullability, default,
  storage, dropped status
- current PK and replica identity
- expected source row set
- current destination, mapping, CH columns, and CH sorting key
- lifecycle policy and expected transition classification

Renderer chooses only PostgreSQL-legal actions from current state. Keep invalid
DDL generation in a separate PostgreSQL-error campaign; syntax and dependency
errors provide no replication confidence

Use small bounded programs for shrink quality:

- one or two tables initially
- one to four columns from restricted type alphabet
- one to four transactions
- one to eight actions per transaction
- small row sets with deliberate boundary values

Start type alphabet with bool, int4, int8, text, varchar, numeric, timestamp,
and bytea. Include NULL, empty values, integer width edges, numeric precision
edges, repeated strings, and external-TOAST-sized strings. Add arrays, JSON,
UUID, inet, intervals, domains, enums, and custom types after core oracle holds

## Architecture

Use two related harnesses

### Pure coverage-guided targets

Extend `fuzz/` crate from [FUZZ.md](FUZZ.md) with semantic targets. Keep each
invocation deterministic and free of PostgreSQL / ClickHouse processes

| target | input | oracle |
|---|---|---|
| `ddl_schema_transition` | descriptor pair or chain | every changed field maps to explicit `Supported`, `NoChEffect`, `Policy`, or `Unsupported`; no silent unclassified drift |
| `ddl_mapping_transition` | descriptor chain + starting mapping | supported columns and names match reference mapping after each transition |
| `ddl_route_transition` | relation lifecycle + namespace config | every `Converge` heap resolves one destination; rename/drop/recreate never silently maps to stale generation |
| `ddl_drain_order` | heaps, schema controls, truncates, subxact order | merged walk preserves control-before-dependent-row and pre-control durability order |
| `ddl_plan_idempotence` | abstract sealed plan + restart cut | replayed mutation sequence converges to uninterrupted model state |
| `ddl_reject_atomicity` | abstract plan containing unsupported transition | validator rejects before first modeled side effect |

Use `arbitrary` for structure generation. Normalize impossible combinations to
smaller valid cases rather than returning early; a target dominated by rejected
inputs produces misleading edge coverage

### Real-cluster state machine

Add `tests/ddl_fuzz_e2e.rs` and helpers under `tests/common/ddl_fuzz/`, reusing
`inproc_harness.rs`. Use seeded generator or `proptest`, not libFuzzer process
loop. External PostgreSQL and ClickHouse branches are invisible to libFuzzer,
and process/WAL switching cost makes per-input target invocation unsuitable

Run multiple isolated programs under unique source namespaces before one WAL
switch. Drain once, then query every case. If stream fails, record current
program prefix and replay it alone. If state comparison fails, replay failing
case on fresh clusters and shrink there

Keep campaign mode separate from normal deterministic CI:

- fixed regression seeds always run under existing runtime skip gate
- bounded deterministic seed range runs in regular live CI
- broader seed range runs in scheduled PG 16 / 17 / 18 jobs
- long campaign records corpus and artifacts without changing source tree

Do not use sleeps as correctness barriers. Wait for dispatched LSN, shadow
replay, emitter ack, and pipeline drain. Query CH only after relevant commit LSN
is acknowledged

## Pure DDL planning seam

Extract schema decision from live `DdlApplicator`. Current code combines event
classification, mapping mutation, SQL rendering, and CH I/O, which forces a
live client to test transition semantics

Target shape:

```rust
struct DdlPlan {
    mutations: Vec<DdlMutation>,
    mapping_delta: MappingDelta,
    disposition: DdlDisposition,
}

enum DdlDisposition {
    Apply,
    NoChEffect,
    Policy,
    Unsupported { reason: UnsupportedDdl },
}

fn plan_schema_transition(
    old: Option<&RelDescriptor>,
    new: Option<&RelDescriptor>,
    mapping: Option<&TableMapping>,
    config: &DdlConfigSnapshot,
) -> Result<DdlPlan, PlanDdlError>;
```

`DdlMutation` should express intent rather than raw SQL:

- create / drop / rename table
- add / drop / rename / modify column
- rebuild-required key or engine change
- no-CH-effect metadata change

Render SQL only after plan validates. Apply `mapping_delta` only after CH
mutation succeeds. Let transaction planner inspect `DdlDisposition` before any
heap or control side effect. This seam makes unsupported type, key, persistence,
and relation-name transitions fuzzable and gives `RejectBeforeEffects` a real
enforcement point

Do not require this extraction before first real-cluster corpus. Deterministic
known-gap tests can land against current behavior, marked `KnownGap`. Require
pure planning seam before claiming broad semantic fuzz coverage

## Differential oracles

### Row state

Reserve immutable surrogate key for first campaign and exclude it from DDL.
Query source into canonical typed rows ordered by key. Query effective CH state
using `_lsn` and `_is_deleted`, not physical part rows. Normalize:

- explicit NULL distinct from type default
- bytea as hex
- numeric as canonical decimal text
- timestamps in UTC with declared precision
- floats by bit pattern, preserving NaN and infinities
- text and custom output as bytes where collation or formatting can vary

Once stable-key campaign passes, enable key mutations and compare through
model-owned logical row identities. Do not group CH by newly expected source
key when target still uses old sorting key; that would hide key-transition bugs

### Schema state

Compare current source descriptor projection with `system.columns`:

- destination existence and name
- mapped column existence and order-independent name set
- CH type and nullability through type bridge
- default where missing-value semantics rely on it
- synthetic column tail
- sorting key / engine metadata when source key transition is in contract

Ignore source CHECK, FK, trigger, and ordinary index metadata unless it changes
replica identity or routing

### Lifecycle state

Track source relation generation independently of name. Assert:

- DROP under `drop` removes destination and runtime-derived mapping
- recreate does not inherit rows from old source generation
- `retain` / `warn` behavior matches configured policy model
- rename or schema move cannot turn a mapped generation into silent unmapped
  discard
- partition attach / detach follows declared fan-in or unsupported contract

### Progress and diagnostics

For every `Converge` case assert:

- emitter ack reaches barrier LSN
- no pipeline fatal
- no rows route unmapped
- no unsupported relation or operation counter moves
- no rejected type-change counter moves
- no ambiguity overlaps emitted rows
- source and CH row/schema oracles match

For `RejectBeforeEffects`, snapshot relevant CH state before transaction and
assert exact equality afterward. A fatal after pre-DDL rows became durable is
partial application, not successful rejection

### Restart equivalence

Compare uninterrupted result with restart at generated cuts:

- after source commit, before pump observes commit
- after plan seal, before execution
- after pre-DDL data durability fence
- after each CH DDL mutation
- after TRUNCATE
- after post-DDL INSERT durability, before ack persistence
- between PREPARE and COMMIT / ROLLBACK PREPARED

Use existing kill/restart hooks where available. Add deterministic failpoints
only at product-owned boundaries; do not infer cuts from wall-clock delays

## Shrinking and artifacts

Represent each program as versioned JSON independent of random generator. Every
failure artifact contains:

- generator seed and program JSON
- rendered SQL by session
- expected classification
- source and CH schema snapshots
- canonical source and CH rows
- relevant LSNs and ack position
- walshadow counters and first fatal message
- PostgreSQL and ClickHouse versions
- walshadow config, especially namespace and drop policy

Implement semantic shrink order:

1. remove unrelated transactions
2. remove unrelated tables
3. remove actions while preserving references
4. collapse savepoint / subxact structure
5. remove columns not used by failing transition
6. reduce row count
7. shrink values toward NULL, zero, empty, width boundary, and short text
8. simplify types while preserving transition class

Repair references after removal through stable `RelId` / `ColumnId`; never let
shrinker turn most candidates into invalid SQL. Emit minimized SQL beside JSON
and promote confirmed bugs into deterministic integration tests

Keep large evolving corpora outside git. Check in small semantic seeds and
minimized regressions. Existing parser corpus policy in [FUZZ.md](FUZZ.md)
continues unchanged

## Semantic coverage

LLVM edge coverage measures walshadow code only; record operation-space
coverage explicitly. Persist counts for:

```text
DDL kind
x DML kind before / after
x same / separate transaction
x commit / abort / savepoint / prepare
x descriptor transition class
x no rewrite / in-place / filenode rewrite
x nullable / non-nullable
x key unchanged / changed / dropped
x toast / inline
x mapped / auto-created / excluded
x drop strategy
x restart cut
x PostgreSQL major
```

Require all legal single operations and ordered operation pairs before growing
program depth. Weight generator toward uncovered pairs and prior bug
neighborhoods. Do not claim confidence from raw case count; report transition
coverage plus unique minimized failures

Track outcome buckets separately:

- converged
- expected policy divergence
- rejected before effects
- rejected after partial effects
- acked mismatch
- fatal mismatch
- unmapped discard
- generator / PostgreSQL rejection

Generator rejection rate must stay low. High rejection means model emits
invalid SQL, not that product survived difficult cases

## Campaign sequencing

### Phase 0, contract and deterministic matrix

- encode `Converge` / `RejectBeforeEffects` / `Policy` / `KnownGap`
- land known transition corpus and supported controls
- add canonical row and schema snapshot helpers
- record counters around each case
- resolve stale assertions or comments claiming DROP / RENAME COLUMN remain
  unimplemented

### Phase 1, descriptor and mapping model

- define transition classifier over every `RelDescriptor` field
- add `ddl_schema_transition` and `ddl_mapping_transition`
- classify relname, persistence, replica identity, defaults, nullability,
  typmod, physical type, relkind, and toast changes explicitly
- turn every unclassified descriptor drift into fuzz assertion

### Phase 2, pure DDL plan

- extract `plan_schema_transition`
- validate unsupported transition before execution
- add abstract CH schema model and SQL-render unit tests
- add route lifecycle, reject atomicity, and idempotent replay fuzz targets

### Phase 3, generated live transactions

- add one-table stable-key generator
- run fixed seeds under PG 16 / 17 / 18
- add savepoints, subxacts, multiple tables, and clean-xact interleave
- promote every failure after semantic minimization

### Phase 4, destructive and physical transitions

- enable key changes, drop/recreate, rewrites, TOAST, unlogged transitions,
  partitions, tablespaces
- connect explicit expected gaps to their owning plans
- add restart cuts and CH reconnect failures

### Phase 5, sustained campaign

- schedule bounded seed ranges per PG major
- persist JSON corpus and semantic coverage report
- deduplicate failures by normalized program plus first divergent oracle
- periodically replay full checked-in regression corpus against supported CH
  versions

## Acceptance

- Every `RelDescriptor` field change has explicit transition classification;
  fuzz target cannot produce silent unclassified drift
- Every supported DDL/DML ordered pair has at least one real-PG regression seed
  on PG 16, 17, and 18
- Generated `Converge` cases assert rows, schema, ack, and zero silent-discard /
  unsupported counters
- Generated unsupported cases reject before any transaction side effect in CH
- Restart replay matches uninterrupted final state for every supported control
  cut
- Known gaps remain executable, minimized, and reported separately; closing one
  moves its seed to `Converge` or `RejectBeforeEffects`
- Failure JSON replays deterministically without original random seed
- Scheduled report publishes semantic pair coverage, outcome buckets, and new
  minimized failures, not only execution count

## Cross-links

- [FUZZ.md](FUZZ.md) — byte parsing, CRC, codec, and C-boundary fuzzing
- [coverage100.md](coverage100.md) — line coverage and live DDL branch matrix
- [catalog_capture_completeness.md](catalog_capture_completeness.md) — relation /
  namespace rename event gaps
- [pinned_ddl_baseline.md](pinned_ddl_baseline.md) — cold-start DDL baseline and
  CH-existence drift
- [two_phase_commit.md](two_phase_commit.md) — prepared transaction restart
  durability
- [TABLESPACES.md](TABLESPACES.md) — non-default tablespace bootstrap and shadow
  path gaps
- [pipeline_backpressure_and_scaling.md](pipeline_backpressure_and_scaling.md) —
  DDL barrier scope and pipeline ordering
- [../desc_log.md](../desc_log.md) — pending descriptor timeline and ambiguity
  fence
- [../emitter.md](../emitter.md) — transaction plan, route freeze, DDL /
  TRUNCATE execution, ack semantics
