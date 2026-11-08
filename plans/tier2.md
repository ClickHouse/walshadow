# Promote container types from oracle to local codecs

Tier 1 reads fixed-width base types straight from tuple bytes. Tier 2 decodes in
process and writes ClickHouse wire columns without PostgreSQL. Tier 3 defers to
shadow PostgreSQL through [conversion client](../src/ops/oracle.rs), described in
[value architecture](../architecture/values.md)

Arrays, `hstore` maps, and pgvector vectors already use tier 3. Promotion removes
shadow conversion work and round trips without changing destination values. One
tier-3 column makes each Native block needing conversion wait on shadow, even
when every other column encodes locally. Measure oracle requests and CPU beside
latency, allocations, and retained bytes

## Why jsonb was cheap and containers are not

Every tier-2 type so far lands in a target whose wire shape is one byte range per
row. `ColumnBuf` offers `Fixed`, `String`, and nullable pairs, and
`ColumnBuf::new_for_ast` chooses between them on `elem_size` alone. ClickHouse
accepts `JSON` as a string body, so `jsonb` needed a decoder and nothing else

Containers break that assumption. `Array(T)` on wire is a `UInt64` offsets column
plus a flattened element column, with a null map when `T` is nullable. Rows
contribute variable element counts to shared storage, so one row no longer maps
to one buffer write. `Array` reports `elem_size` zero, so an array target reaching
`new_for_ast` would silently take the `String` shape and write wrong bytes. Two
guards prevent that: `composite_target` routes `Array`, `Map`, `Object`, and
`QBit` to oracle, and `local_matrix_covers` omits array type OIDs

Commit planning already reads ordinary WAL tuples through borrowed datums and
writes encoded column chunks. Large plans spill those chunks; conversion workers
combine local and oracle columns, then seal prepared Native blocks. Extend this
path with container buffers and a borrowed element walk. Nested owned values
are not a prerequisite. See [direct encoding](native_encoding.md) for remaining
load and TOAST work

## Dependencies need no change

`clickhouse-c` builds and round-trips `Array(Nullable(T))` and nested arrays
through `chc_build_array`, and builds `Array(Tuple(..))` for maps through
`chc_build_tuple`. `clickhouse-c-rs` exposes both as `ColumnBuilder::array` and
`ColumnBuilder::tuple`. Neither blocks this work

One optional upstream change is ergonomic. `ColumnBuilder::array` and
`ColumnBuilder::nullable` store a raw pointer into the child builder, so every
wrap level must outlive the level above it. Conversion workers currently hold
leaves then roots. Containers need deeper builder storage, with tuple pointer
scratch for maps. Choose stable per-level storage or an owning arena after
checking lifetimes. Treat this as builder organization, not a throughput claim

## Work in walshadow

| Area | Change |
|---|---|
| Wire shape | Recursive array buffer with row offsets and child buffer; add tuple children for maps |
| Buffer operations | Append empty/default rows, account allocated capacity, compare shapes, and merge chunks with offsets rebased by child element count |
| Plan spill | Serialize and restore recursive column buffers; bump `PLAN_VERSION` when adding buffer tags |
| Decode | Walk borrowed on-disk `ArrayType` bodies, including null bitmap, into child buffers |
| Adapters | Reuse container encoding from borrowed datums and owned raw pending values; handle COPY text and fast defaults explicitly |
| Gates | Admit only supported source/target pairs and input representations; retain oracle routes for other shapes |
| Type bridge | Preserve existing mappings; define multidimensional behavior against configured target |

Keep raw transaction spill unchanged when existing pending-value forms suffice
Version transaction or deferred-load formats only if their representation changes

Give the walk its own `CodecError` variants for a rejected array header and an
uncovered element type, so a malformed body is distinguishable from a type the
table does not admit

### Pair source shape with target shape

Choose encoding from source type, element codec, and target shape together
Admitting an array OID must not enable local encoding for a `String` override,
unsupported nested target, or `QBit`. Keep those pairs on oracle until covered

Column-wide admission also affects resolved TOAST values and COPY rows.
Resolved values retain raw bodies in existing pending cells; COPY casts
containers to text. Support that text form or introduce an explicit oracle fallback before
opening gates. Fast defaults need equivalent handling. An unsupported input
must not enter a local-only buffer merely because WAL encoding works

### Element metadata

The walk needs element length, alignment, and by-value flag. `RelAttr` carries
those for the column type, which for an array column describes the array rather
than the element, and carries no `typelem`

Start with a static layout table keyed by built-in element OID. Validate header
element OID against admitted source type; type name alone is insufficient

Separate `domain` branch resolves domain base OIDs and typmods during catalog
construction and reads `typelem`, but does not carry element layout into
`RelAttr`. Reuse that resolution when composing changes. Array bodies still
carry original element OIDs, including domain OIDs after descriptor
normalization. Retain enough metadata to resolve those identities at record
boundary, or keep such arrays on oracle. Cover domains over arrays and arrays
of domains separately. Keep pinned worker, SQL snapshot, and source bootstrap
metadata consistent

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
| Multi-dimensional array | Preserve configured target; use oracle for supported nested overrides until local nested encoding exists |
| Non-zero lower bound | Elements in stored order, bounds dropped |
| Element type outside the table | Oracle |

Default mapping uses array type name without inspecting dimensions, so a
multidimensional value does not automatically become `String`. Prove behavior
for default one-layer target, explicit nested target, and `String` override
Reject unsupported shape before emitting a partial row; never change target
schema from a value's dimensions

`decode_text_array` refuses a null bitmap, correct for its own caller and wrong
for a column codec. Runtime configuration reads its own key columns before the
bridge exists and wants `Vec<String>`, so let it reuse the general walk behind a
nulls-refusing adapter, as noted in [runtime configuration](runtime_config.md)

## Staging

1. Add recursive array buffers, merge/accounting/spill operations, and builder
   storage. Prove `Array(Nullable(T))` over fixed and string elements while
   production gates stay closed
2. Add borrowed `ArrayType` walk and element metadata. Reuse it for owned raw
   pending bodies; cover COPY text, fast defaults, compressed and external values
3. Open gates per source/target pair after local/oracle parity across input
   paths. Keep unsupported pairs on oracle
4. Add `vector` and `halfvec` codecs feeding same array buffer with non-nullable
   elements. Their dimension header and float payload bypass `ArrayType`; this
   can be first codec after buffer support if minimizing initial scope
5. Add `hstore`: `Map(String, Nullable(String))` is
   `Array(Tuple(String, Nullable(String)))`, needing tuple children, pointer
   scratch, and expansion equivalent to the module's `hstore_to_matrix`

Keep existing production behavior while gates stay closed. Open each later
family only after proving its supported representations and overrides

## Completion

Require identical destination contents through local and oracle paths for every
admitted element type, across insert, update, delete, empty array, NULL column,
null elements, non-default lower bounds, and TOASTed arrays. Extend
[direct/owned parity](../src/emit/direct_parity.rs), then compare destination
contents against oracle independently across streaming, COPY, and backup loads
Cover raw transaction spill, recursive plan spill, merging chunks with empty and
NULL-bearing rows, and a schema change adding an array column with a fast
default. Test malformed headers separately from unsupported element types

When composing domain support, include original domain element OIDs, nested
domain bases, typmods, and domains created in same transaction as their table
Round-trip prepared container blocks with none/LZ4/ZSTD and compression-mismatch
rebuilds; retain existing INSERT retry and acknowledgement guarantees

Measure oracle round trips removed per table shape before widening element
coverage
