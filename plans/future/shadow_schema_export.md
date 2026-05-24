# shadow schema export

Shadow PG already holds a live, WAL-replayed mirror of source's
catalog (`pg_class`, `pg_attribute`, `pg_type`, `pg_index`,
`pg_constraint`, `pg_depend`, `pg_namespace`, `pg_proc` for
defaults) with no user data. Today nothing consumes that beyond
`ShadowCatalog`'s runtime lookups. Ship shadow's schema as a payload
delivered to a third-party PG cluster in one of two forms: A1 SQL
dump via `pg_dump --schema-only` against shadow's socket; A2 hollow
data directory of empty heaps + indexes, tar'd from shadow's data
dir after a prune pass

## Use case

Third-party Postgres cluster wants source's schema without source
itself. Three plausible consumers:

- **Dev / test cluster bootstrap.** Operator wants "source's schema
  as of LSN X" loaded onto empty PG, no data. Today they run
  `pg_dump --schema-only` against source, which costs source CPU
  and IO and requires production credentials. Shadow already has it,
  costs zero on source, and pins to a specific LSN rather than
  "whenever pg_dump's snapshot fired"
- **Bootstrap material for a fresh full-PG replica.** Pairs with
  sync_commit_witness — when the surviving full-PG node needs
  re-seeding from scratch (or prior full-PG standby is destroyed),
  shadow's schema is the obvious starting point ahead of a
  data-bearing BASE_BACKUP
- **CH schema sync.** The emitter maps PG relations to CH tables via
  TOML config today. A future "auto-create CH tables"
  mode could ride shadow's catalog: new relation on source → shadow
  sees catalog row → walshadow synthesises matching CH `CREATE
  TABLE` before first data block lands

## Two output shapes

### A1 — SQL payload

Walk shadow's catalog, emit DDL text. Closest analogue is `pg_dump
--schema-only`. Shadow runs same PG major as source, so feeding
shadow's output back into `psql` on a matching-major target is
supported by PG itself

Lift: shadow can already produce this directly. `pg_dump
--schema-only -h <shadow-socket> -d <shadow-db>` works today because
shadow holds catalog rows post-replay. Walshadow-side work is
wrapping the invocation: `Shadow::dump_schema(target_lsn) -> String`
gates on `wait_for_replay(target_lsn)` and shells out to `pg_dump`.
~50 LOC plus gating logic shadow already has

Emitted shapes:

- `CREATE SCHEMA` for every namespace except system schemas
- `CREATE TYPE ... AS ENUM` / composite types
- `CREATE SEQUENCE` with `last_value` / `is_called` from
  `pg_sequence`
- `CREATE TABLE` with column list (`pg_attribute` ordered by
  `attnum`, skipping dropped), `NOT NULL` (`attnotnull`), defaults
  (`pg_attrdef`), check constraints (`pg_constraint contype = 'c'`),
  storage clauses
- `ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY / UNIQUE / FOREIGN
  KEY` for `contype IN ('p','u','f')`, ordered topologically on
  `pg_constraint.confrelid` so referenced tables exist first
- `CREATE INDEX ... USING <am> (...) WHERE ...` for `pg_index` rows
  not already constraint-backed
- `CREATE TRIGGER` / `CREATE RULE` / `GRANT` only if explicitly in
  scope; default is "out of scope, document as such"

Wins over `pg_dump` against source:

- Zero load on source PG. Catalog reads against shadow's socket on
  walshadow host
- LSN-scoped: dump reflects source's schema at exactly `target_lsn`,
  useful for reproducing a debug state
- Doesn't require production credentials on the dumping host

Caveats:

- `pg_dump` output assumes target can run its `SELECT
  pg_catalog.set_config(...)` preamble; same constraint as upstream
- Trigger functions live in `pg_proc.prosrc`. Walshadow's filter
  classifies this as catalog (good) but function bodies referencing
  other schemas might fail to compile on a target that doesn't have
  those schemas yet. Standard `pg_dump` ordering problem, not
  walshadow's to solve
- Extension state. `pg_extension` is catalog but `CREATE EXTENSION
  foo` runs the extension's install script which is not in shadow's
  catalog. Shadow knows "extension foo is installed at version X";
  target must have foo's package installed for dump to apply

### A2 — hollow data directory

Same end state expressed differently. Walshadow ships a fully
`initdb`'d, catalog-populated, schema-aware data directory with
every user heap / index file present but zero-row. Effectively
shadow's own data dir, post-prune of any relfilenodes recovery
happened to touch with WAL

Shape:

- Spin shadow in normal (non-recovery) mode briefly
- Per `pg_class`: any `oid >= FirstNormalObjectId` non-catalog
  relfilenode → truncate to one empty page (or unlink; recovery
  doesn't touch these because filter NOOPs their WAL)
- Tag directory with `.walshadow-schema-only` sentinel
- `pg_ctl stop`, tar the data dir

Target operator: `tar -xf schema.tar -C /var/lib/postgresql/data &&
pg_ctl start`. PG comes up with empty tables matching source's
schema. No `psql` replay, no DDL ordering surprises, indexes already
present (and valid, because empty)

Wins over A1:

- No DDL replay step on target. Fastest "schema present, ready for
  inserts" path
- Sequences carry `last_value` byte-exact (sequence state lives in
  heap file, not just in `pg_sequence`)
- Extension on-disk state (function OIDs, type OIDs in `pg_proc` /
  `pg_type`) is byte-exact; subsequent WAL or COPY loads against
  target line up without OID-remap surprises

Caveats:

- Major-version-locked. A1's SQL survives minor-version drift on
  target; A2's data dir doesn't (`pg_control` is major-pinned)
- Storage on shadow during prune pass. Schema-only shadow is
  MiB-scale, but prune sequence needs shadow to spin in read-only
  mode first, which means recovery must catch up to `target_lsn`
  before prune. Acceptable in steady state
- OID skew on target. Source's `pg_class.oid` propagates exactly
  (the win), but target that's already been `initdb`'d with
  conflicting OIDs can't re-use the same data dir. Operator workflow
  is "fresh empty volume + shadow's tar"

## Sequencing between A1 and A2

A1 lands first as a 1-command shim around `pg_dump`. A2 is the
natural follow-up once a heap-prune pass on the shadow data dir
exists (today shadow holds catalog only; an explicit prune pass
isolates the catalog tablespace from any future heap-load
artefact). A2 then reduces to "skip enable_standby_recovery, tar
what's on disk"

## Why deferred

No downstream consumer has asked. Speculative based on capabilities
walshadow already half-has, not on a deployment that's blocked
without it. CH schema sync is the most concrete prospect but the
emitter's TOML mapping covers the production path today. Dev/test bootstrap
is real but `pg_dump --schema-only` against source is already in
operators' muscle memory; the cost of running it on source instead
of shadow is "some CPU and IO" not "blocked workflow". Sync-commit
witness use case (re-seeding a full-PG replica from shadow) is
itself deferred (see `sync_commit_witness.md`); promoting both
ideas together is a natural pairing if either materialises

Neither A1 nor A2 blocks any landed surface. Land when a consumer with
a specific use case forces the matrix

## Dependencies

- A1: only needs `wait_for_replay(target_lsn)` (shadow already
  has this) and a shell-out wrapper. Independent.
- A2: requires a heap-prune pass on the shadow data dir to land.
  A2's output shape is "BASE_BACKUP shadow data dir, pruned, without
  enabling standby recovery"

## Open question

Incremental schema delivery for catalog evolution after first
export. A1's `pg_dump` output is a one-shot snapshot at `target_lsn`;
subsequent source-side DDL doesn't propagate to the target consumer
automatically. Two shapes possible:

- Periodic re-dump. Operator schedules `dump_schema(now_lsn)` on a
  cadence, target consumer runs the diff or full-replaces. Simplest;
  target burns CPU on each apply
- Streaming DDL events. Walshadow already has the `SchemaEvent`
  channel for CH applicator. A future "DDL relay to third-party
  PG" sink would consume the same channel and emit DDL text to a
  remote target as events arrive. Lighter on the target but
  requires a sink with its own connection management and failure
  semantics. Out of scope until a consumer asks

The sourcing decision (per-relation stream vs whole-payload) ties
into which use case promotes the work. CH auto-create wants
per-relation; cluster bootstrap wants whole-payload. Don't pre-judge
which lands first — let the asking consumer choose

## Acceptance

- A1 drill: daemon running with shadow caught up to LSN X. Issue
  `Shadow::dump_schema(X)`; output replays cleanly into a fresh
  matching-major PG via `psql`. Schemas, sequences (with
  `last_value`), tables, indexes, FKs all present and queryable.
  Zero impact on source PG (source connection count unchanged
  during dump)
- A2 drill: shadow primed via the heap-prune pass.
  Tar shadow's data dir, untar onto fresh volume on third-party
  host, `pg_ctl start`. PG starts cleanly, all user tables present
  and empty, indexes valid, sequences carry source's `last_value`.
  Insert into a sequence-backed table assigns the next expected
  value
- Out-of-scope items (triggers, ACLs, extensions requiring source-
  side install scripts) documented as operator preconditions in
  the export tool's `--help`
