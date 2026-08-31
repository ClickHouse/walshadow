# oracle_native_blocks — bridge emits CH-native, not text

## Problem

Today bridge-routed tier-3 values round-trip through **text**: the worker runs
`typoutput` → `ColumnValue::Text`, and the emitter then **re-parses** that text
back into ClickHouse's columnar form. The emitter-side machinery that does this
re-parse — `ColumnBuf::{Array,Map,Json}`, `encode_array`/`encode_map`/
`encode_json` (with `parse_pg_array_1d` / `parse_vector_list` / hstore + JSON
parsers), and the `NodeArena` nested-`ColumnBuilder` builder — is
decode-to-text-then-parse-back, which is wasteful and brittle (PG text quoting,
CH `JSON` object-only + null-slot rules, etc.).

## Approach

Have the resolver produce ClickHouse column data directly, so the emitter
splices it in without a text detour. The extension links a CH serializer
(`clickhouse-c` / `pg-clickhouse-c`); the `DECODE` op carries the **target CH
type** per item and returns native column bytes instead of a `typoutput`
string. The emitter keeps its columnar assembly for scalars but drops the
composite text parsers.

## Batch the DECODE (row-at-a-time → list of rows)

Today the bridge is **per-tuple**: `resolve_pending_tuple` bundles one row's
pending columns into a single `DECODE` request, and the decode pool calls it
once per new/old tuple — one socket round trip per row. That's fine for
scattered text but wrong-shaped for native output: a ClickHouse column is
**many rows of one type laid out together**, so a row-at-a-time worker can't
build a column and every row pays a round trip.

Change the unit of resolution from one row to a **list of rows** (a
column-major batch):

- `DECODE` takes, per pending column, the target CH type + the raw on-disk
  Datum for *every row in the batch* (nulls marked); the worker decodes down a
  column and returns that column's native bytes (values + null map, plus
  offsets for Array/Map) in one shot.
- Resolve at **batch granularity, not tuple granularity**. The batcher already
  accumulates per-table row batches (`InsertBatch`/chunk — see
  [../emitter.md](../emitter.md)); hand a whole table-chunk to the bridge and
  get back native columns ready to append to the block. This amortizes the
  round trip over the batch and matches CH's columnar layout end-to-end.
- Ordering/back-pressure: one in-flight batch request per table-chunk keeps the
  existing seq/ack accounting; size the batch to the inserter's block size.

## Scope / cost

- **pgext** (`pgext/decode.c`, `worker.c`, `walshadow.h`): link the CH
  serializer; `DECODE` request becomes column-batched (target CH type + a list
  of raw Datums per column); response returns the serialized native column
  (values + null map + offsets). Reimplement PG-Datum → CH-native for the
  tier-3 matrix in C.
- **Protocol/coupling**: a PG-side component must now know the destination CH
  type (it knows nothing about ClickHouse today) — carry it in the request or a
  negotiated intermediate.
- **Rust** (`ops/bridge.rs`, `ops/oracle.rs`, `emit/…`): `Bridge::decode`
  takes a batch and returns whole native columns; resolution moves from the
  per-tuple call in `emit/pipeline/decode.rs` to batch granularity alongside
  the batcher. Delete the emitter composite `ColumnBuf` variants + text parsers
  + `NodeArena`; scalar `ColumnBuf`/`build_column` stays.
- Estimated large + higher-risk (new C dep in the PGXS `.so`, native-format
  edge cases across many types, batched-protocol reframe).

## Does NOT solve greenfield

This only changes *where* serialization happens; it still needs a live PG with
the walshadow module to do the decoding. Greenfield has none until after
bootstrap — that is handled by the bootstrap oracle
([../bootstrap.md](../bootstrap.md)), independently of this.

## Alternative considered

In-tree Rust decoders for PG array/hstore/jsonb on-disk binary (no bridge at
all) — self-contained and greenfield-friendly, but re-implements PG's
varlena/alignment/null-bitmap/JEntry formats byte-exact, which is its own large
correctness surface. Native-from-the-extension reuses PG's own rendering and is
preferred where a PG is available.
