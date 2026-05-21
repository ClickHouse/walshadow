# 01 — Read-time defaults (`atthasmissing` + `attmissingval`)

Closes [PLAN.md §"Known correctness gaps" #4](../PLAN.md#known-correctness-gaps).
v1.0 acceptance §1's `ALTER TABLE ... ADD COLUMN c int DEFAULT 7`
fails today: the decoder reads physical tuple bytes and emits NULL
for pre-ALTER rows, while source's `SELECT c FROM t` returns 7
because PG synthesises the default at read time via
`getmissingattr` in `heaptuple.c`

## Why

Fast-path `ADD COLUMN ... DEFAULT k` (PG 11+, baseline for PG 16+)
sets `pg_attribute.atthasmissing = true` and stores the default's
on-disk bytes in `pg_attribute.attmissingval` (an anyarray with one
element). Existing rows are not rewritten; their `t_infomask2` natts
count stays unchanged. PG synthesises the default at read time when
the attnum is beyond the tuple's natts (or null-bit-set for a column
whose attribute has `atthasmissing`)

Walshadow's decoder doesn't consult `attmissingval`, so:
- pre-ALTER rows replicate as NULL for `c`
- post-ALTER UPDATE / INSERT rows replicate correctly (the tuple's
  natts is bumped, value lives in the tuple body)
- source-vs-CH checksum diverges deterministically on pre-ALTER rows

## Surface

`pg_attribute.atthasmissing bool` + `pg_attribute.attmissingval
anyarray` join into
[`RelAttr`](../../src/shadow_catalog.rs). Extend `fetch_attributes`
beside `attnotnull` / `attisdropped`:

```sql
SELECT
    a.attnum::int2,
    a.attname::text,
    a.atttypid::oid,
    a.atttypmod::int4,
    a.attnotnull::bool,
    a.attisdropped::bool,
    a.atthasmissing::bool,
    a.attmissingval,                       -- anyarray, null when not missing
    t.typname::text,
    t.typbyval::bool,
    t.typlen::int2,
    t.typalign::text,
    t.typstorage::text
FROM pg_attribute a
JOIN pg_type t ON t.oid = a.atttypid
WHERE a.attrelid = $1 AND a.attnum >= 1
ORDER BY a.attnum
```

`RelAttr` gains `missing_value: Option<ColumnValue>` decoded once at
descriptor-build time. `attmissingval` is a one-element array of the
column's type; extract `array[1]` via the
[`codecs.rs`](../../src/codecs.rs) array-element path against the
column's type oid

[`heap_decoder::decode_new_tuple_block`](../../src/heap_decoder.rs):
when the tuple's natts is below the descriptor's attnum count, or
the null-bit is set for an attnum whose attribute has `atthasmissing`,
substitute `rel.attrs[i].missing_value.clone()` instead of emitting
`None`. Logic mirrors PG's `getmissingattr`. Physical-NULL on a
non-missing-default column still emits `None`

## Tier-3 fallback

`attmissingval`'s element type can be a Tier-3 type (`jsonb`, `cidr`,
custom domain). Decode via Tier 1/2 codecs when available; for Tier 3
the default falls back to `ColumnValue::PgPending(bytes)` and the
existing [`OracleObserver`](../../src/oracle.rs) path resolves it at
emit time. No new code path

## Tests

Unit ([`heap_decoder.rs`](../../src/heap_decoder.rs) test module):
- `RelAttr` round-trip through `attmissingval` decode for int4 / text
  / numeric defaults
- `decode_new_tuple_block` substitutes the missing value when natts
  is below the attribute count
- Physical-NULL still emits `None` when the attribute has no missing
  default

Integration (`tests/phase14_add_column_default.rs`):
- pre-ALTER INSERT of row `R1`
- `ALTER TABLE t ADD COLUMN c int DEFAULT 7`
- post-ALTER INSERT of row `R2`
- post-ALTER UPDATE of row `R1`
- Drain, then `SELECT id, c FROM t ORDER BY id` on source vs CH must
  agree for both rows

Reuse [`phase8_add_column_replicates_pre_and_post_alter`](../../tests/phase8_e2e.rs)
as the start point — same bootstrap, narrower assertion

## Size

~120 LOC product + ~180 LOC test

## Risks

- **`attmissingval` composite-type decode.** For a column whose type
  is composite, `attmissingval[1]` is a composite Datum. Tier-3
  fallback handles via `PgPending`; verify the oracle path round-trips
  the composite back through `typoutput`. Add a `phase9_oracle`-style
  test for the composite-default case if measurement says it's not
  hypothetical
- **`attmissingval` is `NULL` when `atthasmissing = false`.** Guard
  against decoding NULL as zero-length bytes; the SQL returns SQL
  NULL, tokio-postgres surfaces `None`. `missing_value = None` is
  the correct "physical-NULL means NULL" path
