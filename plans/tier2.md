# Promote container types from oracle to local codecs

Tier 1 reads fixed-width base types straight from tuple bytes. Tier 2 decodes in
process and writes ClickHouse wire columns without PostgreSQL. Tier 3 defers to
shadow PostgreSQL through [conversion client](../src/ops/oracle.rs), described in
[value architecture](../architecture/values.md)

Arrays, `hstore` maps, and pgvector vectors already resolve correctly through
tier 3. Promotion buys latency only. One tier-3 column makes every batch for that
table wait on a shadow round trip, so a single `int[]` column costs the round trip
even when every other column encodes locally. Use `oracle_resolve_seconds_total`
and `oracle_blocks_total` as baseline and acceptance measure

## Why jsonb was cheap and containers are not

Every tier-2 type so far lands in a target whose wire shape is one byte range per
row. `ColumnBuf` offers `Fixed`, `String`, and nullable pairs, and
`ColumnBuf::new_for_ast` chooses between them on `elem_size` alone. ClickHouse
accepts `JSON` as a string body, so `jsonb` needed a decoder and nothing else

Containers break that assumption. `Array(T)` on wire is a `UInt64` offsets column
plus a flattened element column carrying its own null map. Rows contribute
variable element counts to shared element storage, so one row no longer maps to
one buffer write. `Array` reports `elem_size` zero, so an array target reaching
`new_for_ast` would silently take the `String` shape and write wrong bytes. Two
guards prevent that: `composite_target` routes `Kind::Array | Map | Object` to the
oracle, and `local_matrix_covers` omits array type OIDs

Promotion cost is therefore dominated by wire shape and value model, not by codecs

## Dependencies need no change

`clickhouse-c` builds and round-trips `Array(Nullable(T))` and nested arrays
through `chc_build_array`, and builds `Array(Tuple(..))` for maps through
`chc_build_tuple`. `clickhouse-c-rs` exposes both as `ColumnBuilder::array` and
`ColumnBuilder::tuple`. Neither blocks this work

One optional upstream change is ergonomic. `ColumnBuilder::array` and
`ColumnBuilder::nullable` store a raw pointer into the child builder, so every
wrap level must outlive the level above it. The inserter already holds one vector
per level, leaves then roots. `Array(Nullable(T))` needs three levels and `Map`
needs four, so each adds a vector and matching index discipline. An arena owning
nodes and returning indices would collapse those vectors into one structure
Decide it on readability, do not expect throughput from it

## Work in walshadow

| Area | Change |
|---|---|
| Wire shape | `ColumnBuf` array variant holding row offsets plus element buffer, and a build chain deep enough for `Array(Nullable(T))` |
| Value model | Nested `ColumnValue` array variant, spill tag and body encoding, `SPILL_VERSION` bump, recursive `approx_bytes` |
| Decode | Generalize on-disk `ArrayType` walk over element type, including null bitmap |
| Gates | Admit covered array OIDs in `local_matrix_covers`, narrow `composite_target` to shapes still lacking a local buffer |
| Type bridge | None, `array_ch_type` already fixes target types |

Give the walk its own `CodecError` variants for a rejected array header and an
uncovered element type, so a malformed body is distinguishable from a type the
table does not admit

### Pair source shape with target shape

`ColumnEncoding::choose` tests target and source independently. Once array OIDs
pass `local_matrix_covers`, an operator pin mapping an array column to `String`
would select local encoding with no buffer able to hold the value. Make the
choice consider both, so a pin to a scalar target still routes to the oracle
That keeps existing table configuration as the recovery valve for a divergence,
with no release needed

### Element metadata

The walk needs element length, alignment, and by-value flag. `RelAttr` carries
those for the column type, which for an array column describes the array rather
than the element, and carries no `typelem`

Prefer a static element table keyed by array type name, matching the mapping
`type_bridge::array_ch_type` already performs. Built-in array element OIDs are
fixed, and one table can supply length and alignment beside the ClickHouse type
This leaves both catalog paths untouched. Add `typelem` and element metadata to
`RelAttr` only when domains or extension arrays need coverage, and expect the
pinned worker and SQL snapshot paths to agree once it exists

Wrong alignment misreads silently instead of failing. Take alignment per element
from PostgreSQL `src/include/catalog/pg_type.dat` and assert it per type, rather
than reasoning from width

### Null and empty semantics

ClickHouse forbids `Nullable(Array)`, so `type_bridge::map` leaves array columns
non-nullable and a NULL array column arrives as an empty array. Local path must
reproduce results `tests/oracle_types_e2e.rs` already asserts for tier 3

| Source | Target cell |
|---|---|
| NULL column | Empty array |
| Empty array | Empty array |
| Array holding NULL elements | Null elements in `Array(Nullable(T))` |
| Multi-dimensional array | Oracle, target stays `String` |
| Non-zero lower bound | Elements in stored order, bounds dropped |
| Element type outside the table | Oracle |

`decode_text_array` refuses a null bitmap, correct for its own caller and wrong
for a column codec. Runtime configuration reads its own key columns before the
bridge exists and wants `Vec<String>`, so let it reuse the general walk behind a
nulls-refusing adapter, as noted in [runtime configuration](runtime_config.md)

## Staging

1. Decode only: general `ArrayType` walk, nested `ColumnValue`, spill round trip,
   element metadata table. Gates stay closed, so the oracle keeps serving arrays
   and behavior does not move
2. Wire shape: array `ColumnBuf` and the deeper build chain, proven on
   `Array(Nullable(T))` over both fixed and string elements
3. Open gates per element family, paired source and target shapes first
4. `vector` and `halfvec`: pgvector stores a dimension header plus float payload
   rather than `ArrayType`, so it needs its own codec feeding the same array
   buffer with non-nullable elements
5. `hstore`: `Map(String, Nullable(String))` is
   `Array(Tuple(String, Nullable(String)))`, needing tuple children, pointer
   scratch, and expansion equivalent to the module's `hstore_to_matrix`

Steps 1 and 2 carry no divergence risk because gates stay shut. Step 3 is where
divergence can first reach a destination

## Completion

Require identical destination contents through local and oracle paths for every
admitted element type, across insert, update, delete, empty array, NULL column,
null elements, and TOASTed arrays. Cover spill and restore of nested values, and
a schema change adding an array column with a fast default

Measure oracle round trips removed per table shape before widening element
coverage
