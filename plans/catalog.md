# Catalog completeness

Commit and command-boundary capture already provide durable descriptor history
Remaining work concerns missing evidence and catalog changes that do not name
every affected relation. Start in [capture](../src/source/catalog_capture.rs)
and [transaction resolution](../src/xact/xact_buffer.rs)

## Missing tuple payload

A markerless record can be tracked without its payload while its relation is
unknown. If that relation resolves to an ordinary surviving table at commit,
resolution currently warns that rows were not mirrored and continues

Return an error for that outcome. Preserve legitimate discard behavior when
relation was born and dropped within transaction. Test both branches and prove
failure cannot advance durable progress past lost rows

Recovering such rows requires complete creation markers or retaining enough
record evidence to distinguish incomplete observation from a discarded relation
Keep recovery work separate from immediate rejection guard

## Type renames

ALTER TYPE RENAME can leave stored type names stale because type invalidations
do not necessarily enumerate dependent relations. Physical tuple layout stays
usable, but destination mapping can depend on type name

Recapture affected relations from type dependencies or type invalidation probes
Avoid unconditional capture of every relation on every pg_type write, since
ordinary table creation writes types too. Test a type rename without subsequent
table DDL, including replay after restart. Relation and schema renames belong in
[schema transition work](schema.md)

## TOAST rotation cleanup

Retiring a filenode does not currently enqueue relation-drop cleanup. Determine
which old generation data remains after existing rewrite barriers and merges
before adding cleanup

Retirement ledger keys whole mirrors by relation OID. Reusing that operation
for an old filenode can erase a new generation under same OID. Any additional
cleanup must identify old generation and wait until persisted restart floor
passes every possible referrer. Test rewrite, subsequent writes, and restart
around cleanup, not just mirror size reduction

## Capture dependencies

Keep this dependency map when changing capture granularity or schema comparison

| Catalog | Descriptor evidence | Capture concern |
|---|---|---|
| `pg_class` | OID, physical locator, TOAST owner, kind, persistence, relation name | Track speculative creation/rotation and resolve transaction outcome |
| `pg_attribute` | Physical layout, names, typmods, defaults metadata | Preserve command-boundary versions for DML before and after ALTER |
| `pg_index` | Replica-identity key attributes | Feed key changes into schema validation before destination effects |
| `pg_namespace` | Embedded namespace name | Preserve broad recapture when dependent relation invalidations are absent |
| `pg_type` | Embedded type names and dependent type metadata | Resolve affected relations without recapturing all tables for ordinary CREATE |
| `pg_database` | Resolved default tablespace | Recapture physical locators after database move |

For type rename, compare a type-OID-to-relation reverse index with shadow-side
probes of type syscache invalidations. Syscache invalidations can carry hash
keys rather than object OIDs, so a reverse probe needs explicit collision and
cost handling. Audit syscache IDs per PostgreSQL major. A heuristic based on
absence of relcache invalidations needs tests with rename plus unrelated DDL
in one transaction before it can establish completeness

Coordinate relation/schema rename detection with [schema planner](schema.md),
fresh descriptors alone do not establish destination rename policy. Use full
physical locators from [tablespace plan](tablespaces.md) in new reverse maps
and generation cleanup. Keep committed history separate from speculative
[shadow TOAST routing](shadow_toast.md) needed before catalog rows are queryable
