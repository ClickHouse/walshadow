# Promote container types from oracle to local codecs

Tier 1 reads fixed-width base types straight from tuple bytes. Tier 2 decodes in
process and writes ClickHouse wire columns without PostgreSQL. Tier 3 defers to
shadow PostgreSQL through [conversion client](../src/ops/oracle.rs), described in
[value architecture](../architecture/values.md)

One-dimensional built-in arrays whose element table entry matches target element
kind use tier 2. `hstore` maps, pgvector vectors, multidimensional arrays into
nested targets, and arrays over other element types use tier 3. Promotion
removes shadow conversion work and round trips without changing destination
values. One tier-3 column makes each Native block needing conversion wait on
shadow, even when every other column encodes locally

`benches/array_encode.rs` prices one array column built locally against one
`ENCODE_NATIVE` round trip, with daemon and shadow CPU. Extend it per family
before opening gates, and before widening element coverage

## Local array path

`ColumnBuf::Array` holds row end offsets and a child buffer; merges rebase
offsets by child element count. `ArrayBody` walks borrowed on-disk `ArrayType`
bodies against `ARRAY_ELEMS`, a static layout table of element length,
alignment, and by-value flag taken from PostgreSQL
`src/include/catalog/pg_type.dat`. `local_array` admits a column only when
source element and target element kind agree; other pairs keep oracle. COPY
stores `array_send` through `array_from_send`, so every input path reaches the
same walk. `with_roots` builds each wrapper level in its own stack frame,
because `clickhouse-c-rs` wrappers borrow their child builder

New families reuse this path: vectors feed the array buffer, maps add tuple
children

## Remaining work

| Area | Change |
|---|---|
| Vectors | `vector` and `halfvec` codecs read dimension header and float payload into array buffer with non-nullable elements, bypassing `ArrayType` |
| Maps | Tuple child buffer with its own chunk tag; `Map(String, Nullable(String))` is `Array(Tuple(String, Nullable(String)))`, expanding `hstore` as the module's `hstore_to_matrix` does |
| Builder storage | Tuple builders take pointer scratch over children; extend `with_roots` levels to hold it |
| Element coverage | Add `ARRAY_ELEMS` entries with element encoders, eg temporal and numeric elements; each needs local/oracle parity |
| Nested arrays | Recursive array children for explicit nested targets; until then such overrides stay on oracle and one-layer targets reject multidimensional values |

Keep raw transaction spill unchanged when existing pending-value forms suffice
Version transaction or deferred-load formats only if their representation changes

### Domains

Array bodies carry original element OIDs, including domain OIDs after
descriptor normalization, so arrays of domains miss `ARRAY_ELEMS` and stay on
oracle. Separate `domain` branch resolves domain base OIDs and typmods during
catalog construction and reads `typelem`, but does not carry element layout
into `RelAttr`. Reuse that resolution when composing changes, retaining enough
metadata to resolve element identities at record boundary. Cover domains over
arrays and arrays of domains separately. Keep pinned worker, SQL snapshot, and
source bootstrap metadata consistent

### Null and empty semantics

ClickHouse forbids `Nullable(Array)` and `Nullable(Map)`, so a NULL vector or
map column arrives as an empty array or map, matching the oracle. Reject
unsupported shapes before emitting a partial row; never change target schema
from a value's contents

## Staging

1. Add `vector` and `halfvec` codecs feeding array buffer
2. Add tuple buffers, builder scratch, and `hstore`
3. Widen array element coverage and nested targets where benchmark shows
   oracle cost

Open each family only after proving its supported representations and
overrides

## Completion

Require identical destination contents through local and oracle paths for every
admitted type, across insert, update, delete, empty value, NULL column, null
elements, and TOASTed values. Extend
[direct/owned parity](../src/emit/direct_parity.rs), then compare destination
contents against oracle independently across streaming, COPY, and backup loads
Cover raw transaction spill, plan spill, merging chunks with empty and
NULL-bearing rows, and a fast default. Test malformed bodies separately from
unsupported element types. Round-trip prepared blocks with none/LZ4/ZSTD and
compression-mismatch rebuilds; retain existing INSERT retry and acknowledgement
guarantees

When composing domain support, include original domain element OIDs, nested
domain bases, typmods, and domains created in same transaction as their table
