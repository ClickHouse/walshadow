# oracle — differential decode oracle for Tier 3 types

Sits at [`src/oracle.rs`](../src/oracle.rs) plus [`pgext/`](../pgext/).

## Purpose

Tier 3 types are where in-tree decoders diverge from PG on edge cases:
on-disk varlena layouts shift between PG versions, `typoutput`
formatting carries locale baggage, custom typmod paths exist that
walshadow doesn't reimplement. Oracle's job: ship known-stable types
in-tree, route everything else through a shadow-PG bridge that calls
the same `typoutput` PG itself would call.

The validation half is symmetric. Take walshadow's locally-decoded
text, hand the raw on-disk bytes to shadow PG via the same bridge,
diff. Mismatch surfaces decoder regression on the first sampled row of
that type, not after the bad data has settled in CH.

## In-tree Tier 3

`numeric`, `inet`, `cidr`, `interval`. Decoded by
[`src/codecs.rs`](../src/codecs.rs); see also [decoder.md](decoder.md)
for PgPending dispatch surrounding them.

Why these specifically:

- stable wire format across PG versions walshadow targets
- mechanical conversion (no per-row libpq round-trip needed)
- locale-independent text rendering once `lc_numeric` is pinned

`numeric` carries NaN / ±Infinity sentinels; `inet` vs `cidr`
disambiguation lives at the type-OID level not in the body bytes (on-disk
vs wire confusion surfaced here historically).

## Extension-routed Tier 3

`jsonb`, arrays, `tsvector`, ranges, domains. Heap decoder emits
[`ColumnValue::PgPending { type_oid, raw }`](../src/heap_decoder.rs);
oracle's [`resolve_pending_tuple`](../src/oracle.rs) walks tuple
columns, calls `walshadow_decode_disk(oid, bytea) -> text` on shadow,
swaps `PgPending` for `Text` on success.

Extension is **optional**. Absent: resolver returns `Ok(None)`,
`PgPending` stays put, emitter writes raw on-disk bytes verbatim into
CH (`encode_value`'s `PgPending` arm calls `append_string_bytes(raw)`).
No failure, no operator action. Installation enables text rendering
later; daemon picks up the extension on reconnect.

The extension was preferred over two alternatives considered (insert
+ select round-trip; `SELECT $1::bytea::<typ>::text`) because both
require reconstructing wire format from on-disk format — the same
codec work the extension elides.

## walshadow PG extension

Lives at [`pgext/`](../pgext/), built via PGXS.

Surface (one function):

```sql
walshadow_decode_disk(typoid oid, raw bytea) RETURNS text
STRICT IMMUTABLE
```

Reconstructs Datum from on-disk bytes per typlen / typbyval, runs
`OidOutputFunctionCall` on the type's `typoutput`. Branches in
[`pgext/walshadow.c`](../pgext/walshadow.c):

- `typlen == -1` (varlena): bytea body reused as same-shape Datum
- `typlen == -2` (cstring): NUL-terminate then PointerGetDatum
- `typbyval` fixed: memcpy low bytes into Datum slot
- fixed by-ref: palloc + memcpy typlen bytes

Files:

- [`pgext/walshadow.c`](../pgext/walshadow.c) — C function (~125 LOC)
- [`pgext/walshadow.control`](../pgext/walshadow.control) — extension
  metadata, `default_version = '0.1'`, `relocatable = true`
- [`pgext/walshadow--0.1.sql`](../pgext/walshadow--0.1.sql) — DDL
  declaring the C function `STRICT IMMUTABLE`
- [`pgext/Makefile`](../pgext/Makefile) — PGXS-driven, `REGRESS =
  walshadow` for pg_regress

Installed into **shadow PG** today. The runtime-config-from-PG work
([future/runtime_config_from_pg.md](future/runtime_config_from_pg.md))
plans to relocate install to source side; oracle's resolver code
changes only at the conninfo level.

## --validate <N> sampling

Off by default (`rate == 0` short-circuits before any atomic op).
`walshadow-stream --validate <N>` probes 1-in-N tuples through
[`Sampler::pick`](../src/oracle.rs) via `AtomicU64::fetch_add(Relaxed)`
— lock-free, multi-worker safe.

Symmetric probe shape:

1. encode walshadow-decoded value back to PG wire-form bytes (today
   reuses on-disk raw for `PgPending`; in-tree Tier 3 values still
   carry their source bytes alongside decoded text)
2. `SELECT walshadow_decode_disk($1::oid, $2::bytea)` on shadow
3. compare returned text to walshadow's local rendering
4. bump `OracleStats.{probes, matches, mismatches, errors}`

Mismatch is a watchdog signal, not a gate — row still ships to CH.
First sampled bad row of a regressed type surfaces in the status line
via `OracleStats::summary`.

For pure `PgPending` types there is no local text to diff against; the
validator's role for those types reduces to "did the extension call
succeed" — folded into the resolver's success/error counters today.

Operator policy: `--validate 1000` is ~0.1% sampling, invisible
against shadow's existing catalog query load.

## Pinning shadow locale

`lc_numeric` and `lc_time` pinned at shadow bootstrap. Without this,
`typoutput` on `numeric` and `interval` would diff against walshadow's
locale-independent rendering and the validator would noise on
deployments running non-`C` locales. See [shadow.md](shadow.md) for
bootstrap surface.

## Cross-links

- [decoder.md](decoder.md) — `ColumnValue::PgPending` dispatch + Tier
  3 routing through `heap_decoder`
- [shadow.md](shadow.md) — extension install site,
  `try_load_oracle_extension`, lc_* pinning
- [future/runtime_config_from_pg.md](future/runtime_config_from_pg.md)
  — wants extension reachable on source PG
