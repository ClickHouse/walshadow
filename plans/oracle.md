# oracle — Native columns for types walshadow does not decode

[`src/ops/oracle.rs`](../src/ops/oracle.rs) plus [`pgext/`](../pgext/)

![oracle](../architecture/oracle.svg)

## Purpose

Some source types are where in-tree decoders diverge from PG on edge cases:
on-disk varlena layouts shift between PG versions, composite values carry
quoting and nesting rules, custom typmod paths exist walshadow doesn't
reimplement. Ship known-stable types in-tree; hand everything else to shadow
PG, which reconstructs the Datum and converts it straight into a ClickHouse
Native column

Ownership is the point. PostgreSQL owns Datum interpretation,
[`pg-clickhouse-c`](https://github.com/ClickHouse/pg-clickhouse-c) owns the
PG-to-CH conversion, `clickhouse-c` owns the Native format. walshadow never
interprets array elements, hstore pairs, JSON text, null maps, or nested
offsets

Resolution runs at sealed-batch granularity, one request per `InsertBatch`,
after the batcher has fixed row order and before the inserter opens its
ClickHouse query. It is not best-effort: one Datum the worker cannot convert
fails the whole batch, because a substituted value is one the destination
cannot tell from real data

The oracle's answer reflects shadow's catalog at resolve time, which may lag
the row's own catalog interval in DDL edge cases — accepted in exchange for
supporting types walshadow has no codec for

## Local matrix

Fixed-width scalars, `bytea`, `text` / `varchar` / `bpchar`, `numeric`,
`inet`, `cidr`, `interval`, `json`, timestamps, `uuid`. Decoded by
[`src/decode/codecs.rs`](../src/decode/codecs.rs);
[`local_matrix_covers`](../src/decode/heap_decoder.rs) is the single predicate the
emit plan routes on, kept in step with the decoder's own arms by
`local_matrix_agrees_with_decoder`

Why these:

- stable wire format across PG versions walshadow targets
- mechanical conversion, no per-row round trip needed
- flat wire shape ClickHouse takes as a fixed or string slab

`numeric` carries NaN / ±Infinity sentinels; `inet` vs `cidr` disambiguation
lives at type-OID level not body bytes (on-disk vs wire confusion surfaced
here historically)

## What routes to the oracle

[`ColumnEncoding::choose`](../src/emit/ch_emitter.rs) decides once per column, from
the source attribute and the final target type, and never rediscovers it from
a row's value:

- the local matrix does not cover the source attribute — `jsonb`, arrays,
  `hstore`, `tsvector`, ranges, domains, enums, `name`, extension types
- or the target needs PG-aware composite conversion — `Array`, `Map`, `JSON`,
  `Object`, under an optional `Nullable`

Everything else stays local. A column that routes locally but arrives holding
raw on-disk bytes is a plan bug and errors rather than shipping the bytes as a
String

PostGIS `geography` / `geometry` route to the oracle like any other extension
type, but `render_ext_columns` (`src/ops/oracle.rs`) still claims 2-D points
in-tree first, matched on `RelAttr.type_name` because their OIDs are dynamic.
A rendered `POINT(x y)` crosses as a literal cell and lands verbatim; anything
it does not claim crosses as on-disk bytes and gets `typoutput`. `ST_AsText`
semantics are what the `String` mapping wants, and `typoutput` would give
HEXEWKB instead

pgvector `vector` / `halfvec` need no special case: pgvector registers an
explicit cast to `real[]`, so a `Array(Float32)` target converts through PG's
own cast machinery

Every producer — the live decode pool, the COPY / object-store backfills, and
greenfield bootstrap — leaves oracle columns as on-disk bytes and lets the
inserter convert them, so backfilled rows land identically to streamed ones.
**Greenfield bootstrap is the exception in one respect**: it runs before the
shadow exists, so its tail holds a throwaway OID-exact side PG instead — see
the bootstrap oracle in [bootstrap.md](bootstrap.md)

## Protocol v2

One `ENCODE_NATIVE` request (opcode `0x02`) per sealed batch carrying oracle
columns. Outer framing is unchanged; see
[`pgext/walshadow.h`](../pgext/walshadow.h) for the constants both sides share

```text
request  = be_u32 n_rows | be_u32 n_columns
           n_columns × ( be_u32 source_type_oid   # 0 only when every cell defaults
                       | be_i32 source_typmod
                       | lenstr output_name
                       | lenstr target_ch_type )  # canonical, from the daemon's TypeAst
           n_rows × n_columns × cell

cell     = 0x00                                   # Default: SQL NULL or absent field
         | 0x01 be_u32 len bytes                  # DiskRaw: Datum body, varlena header stripped
         | 0x02 be_u32 len bytes                  # TextInput: canonical PG text, eg attmissingval
         | 0x03 be_u32 len bytes                  # Literal: value the daemon already rendered

response = WS_STATUS_OK | one locally framed Native block, to the end of the frame
```

All metadata precedes all cells so worker builds its `pgch_writer` before
reading a value. Cells are row-major. Worker checkpoints writer at each row,
keeps checkpoint storage for next row after every column appends, and rolls
whole row back when conversion raises. Completed rows remain a valid block
prefix, so pgext can narrow later handling without reimplementing
pg-clickhouse-c column trees

The Native block repeats names, types, column count, and row count. That
repetition is the integrity check: the daemon requires every response column
to sit at its requested position, under its requested name, with a type string
byte-identical to the one it asked for. Both sides render canonical type names
through `clickhouse-c`, so a difference means the two pins disagree

Response bytes are untrusted despite the owner-only socket: a wrong block
would put mismatched values under the right column name. `Block::validate`
runs, a second read must reach clean EOF, and no ClickHouse query starts until
all of that passes

## Splicing

The inserter decodes the response once with `clickhouse-c-rs`, then builds the
final block over three kinds of column:

- local fixed / string / nullable slabs, through `build_leaf` + `build_root`
- decoded oracle column trees, through `BlockBuilder::append_column`
- synthetic `_lsn` / `_xid` / `_commit_ts` / `_is_deleted` slabs

`append_column` borrows the decoded tree, so the writer re-emits the same
offsets, null maps, and data slabs the worker wrote, under the inserter's own
expected `TypeAst`. Nothing visits a value. The response block outlives the
whole `send_with_retry` loop, so a ClickHouse reconnect resends the same
columns rather than asking the oracle again

## walshadow PG module

Lives at [`pgext/`](../pgext/), built via PGXS. Not an extension: no control
file, no SQL script, no `pg_proc` row. Shadow's catalog is a read-only physical
copy of source's, so `CREATE EXTENSION` can never run there; the module is
reached only through `shared_preload_libraries`. Module is required on every
catalog shadow; walshadow writes preload settings into shadows it owns

Files:

- [`pgext/native.c`](../pgext/native.c) — Datum reconstruction, source
  adapters, writer, Native response. Also the one translation unit compiling
  `pg-clickhouse-c` and `clickhouse-c`
- [`pgext/overlay.c`](../pgext/overlay.c) — `SnapshotAny` catalog projections
- [`pgext/worker.c`](../pgext/worker.c) — worker registration, socket, framing
- [`pgext/pg-clickhouse-c`](../pgext/pg-clickhouse-c) — unmodified upstream,
  pinned as a recursive submodule carrying its own `clickhouse-c`
- [`pgext/Makefile`](../pgext/Makefile) — PGXS-driven, `MODULE_big` only
- [`pgext/README.md`](../pgext/README.md) — build and load steps, nothing
  about behavior

Loaded into **shadow PG**; stays shadow-only. The worker opens one Unix socket
and never links the daemon or reaches ClickHouse

Missing conversion gets narrow source adapter in `native.c` built on PG APIs,
or the type stays unsupported. One adapter exists:

- **hstore** — a `Map` target has no PG `record[]` cast, so the one
  `hstore_to_matrix` accepting the source type and owned by the same extension
  supplies the key/value matrix. Resolving by extension membership keeps a
  relocated hstore working; an ambiguous match is refused

Cast arity is not walshadow policy: the dependency drives `f(value)`,
`f(value, typmod)`, and `f(value, typmod, explicit)` cast functions itself

## Datum reconstruction

`ws_reconstruct_datum` rebuilds the Datum from on-disk bytes per typlen /
typbyval. Four branches: varlena (fresh 4-byte header around the body),
cstring, typbyval fixed (memcpy into a Datum slot), fixed by-ref (palloc).
`TextInput` cells go through the type's `typinput` with the requested typmod
instead

## Atomicity

Writer has request lifetime; pgext owns one reusable checkpoint.
pg-clickhouse-c records every node buffer length at row boundary; rollback
restores fixed and string data, nested offsets, tuple children, null maps, and
low-cardinality slabs without copying retained rows. One `ERROR` still escapes
to worker's request `PG_TRY`, aborting transaction and resetting response, so
no partial block reaches daemon. `ErrorContextCallback` names failing request
column and row index. Raw value is never copied into error

## Decode environment

Conversion is not a pure function of the bytes: `timestamptz` follows
`TimeZone`, dates `DateStyle`, `interval` `IntervalStyle`, `bytea`
`bytea_output`, floats `extra_float_digits`. The worker's connection would
otherwise inherit whatever database and role defaults replicated from source,
so it pins `UTC` / `ISO, MDY` / `postgres` / `1` / `hex` at startup under
`PGC_S_OVERRIDE`, which no reload or replicated `ALTER DATABASE ... SET` can
shift. Canonical output is what makes ClickHouse input deterministic; matching
the source session instead would need metadata WAL does not carry

## Catalog reads

`ENCODE_NATIVE` is one of four ops the socket carries; `SCAN` is the other one
with a daemon consumer. It projects `pg_class`, `pg_attribute`, `pg_index`,
`pg_namespace` and `pg_type` under `SnapshotAny`, filtered to what one
transaction sees: rows it inserted are present, rows it deleted are not, and
another transaction's uncommitted rows are not. Values are each type's text
output form; `ShadowCatalog` assembles them into descriptors
([shadow.md](shadow.md))

Two arguments turn the same scan into the committed read the daemon captures at
a catalog commit. A top xid of `0` owns no transaction, so every in-progress
writer is foreign and the predicate degenerates to what an MVCC snapshot would
see. An empty oid list reads the whole catalog, which is the only mode
`pg_namespace` and `pg_type` ever had. One scan, so committed and uncommitted
descriptors cannot be built from different rules

The worker never opens the target relation — the replaying transaction holds
AccessExclusiveLock on it and `relation_open` would block against recovery.
Catalog `table_open` plus `systable_beginscan` is the whole surface, and
standby lock replay records AccessExclusiveLocks alone, so AccessShareLock on a
catalog cannot conflict with the DDL in flight. `walshadow.lock_timeout_ms`
bounds the one shape that argument misses: a replaying transaction holding
AccessExclusiveLock on a catalog itself

Which xids count as the requested transaction's is the hard part. Standby
`pg_subtrans` is only as complete as the `XLOG_XACT_ASSIGNMENT` records that
reached it, emitted past 64 cached subxids, so an ordinary savepoint leaves a
subxact with no recorded parent — indistinguishable from a foreign top-level
writer. A chain rooting elsewhere is proof of foreign; no recorded parent is
proof of nothing. Relation-scoped scans take the oid list as the lock argument
and trust an unresolvable writer as theirs, counting it in
`subtrans_mismatch`. Whole-catalog scans have no such argument and fail the
request as inconclusive, so an ok status always means every row returned is
bound to the requested transaction. Losing the oid list loses the lock argument
with it, which is why an empty list is only safe on the committed read — there
is no tree to misattribute a writer to

## Failure semantics

Daemon requires worker at startup. `--bridge-socket` defaults to
`<shadow-socket-dir>/walshadow-bridge.sock`; failure to connect within shadow
connect budget aborts startup

Three failures, none of which can substitute data:

- **cell conversion** — short body, unknown OID, failed cast, a
  multidimensional array against a one-layer target, invalid JSON. Aborts the
  request, counts `conversion_errors`, non-retryable. No ClickHouse query
  begins and no row is acknowledged
- **request or response semantics** — target type the dependency cannot write,
  wrong response schema, malformed Native block. Non-retryable, trips the
  pipeline fatal before the query, leaves the seq unacknowledged so a
  supervisor restart replays the same batch
- **transport** — the bridge redials and replays one read-only request; past
  that the inserter's bounded retry policy applies, and exhaustion trips fatal
  with the ack floor unchanged

There is no fall back to local composite encoding. Retaining that path would
make a value depend on worker uptime

Counters: `walshadow_oracle_{blocks,rows,cells}_total`,
`walshadow_oracle_conversion_errors_total`, `walshadow_oracle_errors_total`.
Native volume counts once, on the bridge, as
`walshadow_bridge_native_bytes_total`

## Backpressure

Raw bodies live in the sealed batch rather than in decode-worker values, and
`TableEncoder::approx_bytes` counts them. The oracle request also rides one
bridge frame, a ceiling the configurable ClickHouse byte budget knows nothing
about. `append_row` sizes a row's cells before touching a buffer, so a row the
pending request cannot hold seals the batch and opens the next one:
`ORACLE_BATCH_SEAL_BYTES` bounds resident request bytes without bounding a row.
Only a row whose own request would not frame is refused, and the frame ceiling
(`WS_MAX_REQUEST_BYTES`, mirrored by the bridge's `MAX_REQUEST_BYTES`) sits well
above `inline_value_max` so a toast value the pipeline admits always frames

Removing per-row resolution from the decode pool removes every bridge await
from decode workers: their throughput and ordering no longer depend on bridge
RTT

## Pinning shadow locale

`lc_numeric` and `lc_time` pinned at shadow bootstrap. Without this, PG's own
text rendering of `numeric` and `interval` would diff against walshadow's
locale-independent rendering on deployments running non-`C` locales.
See [shadow.md](shadow.md) for bootstrap surface

## Cross-links

- [decoder.md](decoder.md) — `ColumnValue::PgPending` dispatch + routing
  through `heap_decoder`
- [emitter.md](emitter.md) — where the sealed-batch resolution stage sits
- [shadow.md](shadow.md) — where the worker's GUCs get written, lc_* pinning
