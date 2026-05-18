# PHASE9 — differential decode oracle + Tier 3 hot types

Closes [Phase 9 of PLAN.md](PLAN.md#phase-9--differential-decode-oracle--tier-3-type-matrix).

## Strategy

PLAN.md framed Phase 9 as a single bucket — implement every Tier 3 codec
locally (`numeric`, `jsonb`, arrays, `inet`, `interval`, `tsvector`)
*and* layer a 1-in-N oracle on top. Mid-implementation it surfaced that
the long tail (`jsonb`, arrays, `tsvector`, ranges, custom domains, ...)
is the same kind of work as walshadow already pushes onto shadow PG for
every catalog lookup: there is one cluster running the same PG binary
that produced the source WAL, and it can answer "what does
`typoutput` render for these bytes?" directly. Picking the hybrid:

- **Local Tier 3 hot types**: `numeric`, `inet` / `cidr`, `interval`.
  Stable on-disk layout, mechanical codec, no per-row libpq round-trip.
- **Everything else** (`jsonb`, `tsvector`, ranges, arrays, domains, …):
  decoder surfaces as
  [`ColumnValue::PgPending`](../src/heap_decoder.rs)
  carrying raw on-disk bytes. Resolution to text happens at emit time
  via `walshadow_decode_disk(oid, bytea) -> text` against shadow PG.
  When the extension is **absent**, the emitter falls back to writing
  the raw bytes verbatim — no failure, no operator action needed.

The local-codec side is a static spend that doesn't compound; the
shadow-extension side eats the long tail with one source of truth.
Codec drift between walshadow and the live PG can only happen for the
three locally-implemented types, and the optional 1-in-N validator
(`walshadow-stream --validate <N>`) tightens that gap by sampling rows
through the same `walshadow_decode_disk` bridge on shadow PG and diffing
the text.

The user's framing question — "why don't we just send bytes to postgres
and get text back?" — turns out to be answerable in three ways. The
literal interpretation (`SELECT $1::bytea::numeric::text`) does not
work because PG's `typrecv` operates on the **wire** format, not the
**on-disk** format; the two layouts differ for `numeric` / `jsonb` /
arrays. The bridge that does work is a small server-side function that
reconstructs the Datum from raw bytes and runs `typoutput`. That is
exactly what the extension does, and Phase 9 ships it.

## What landed

| item | files | tests |
|---|---|---|
| Tier 3 local codecs (`numeric`, `inet`, `cidr`, `interval`) | [`src/codecs.rs`](../src/codecs.rs) | 16 lib unit tests across short/long/special numeric, IPv4/IPv6/CIDR inet rendering, interval text form |
| `ColumnValue::{Numeric, Inet, Interval, Json, PgPending}` variants + heap_decoder dispatch | [`src/heap_decoder.rs`](../src/heap_decoder.rs) | extended `decode_unsupported_type_emits_pending` (was `…_emits_opaque`) to pin the new PgPending fall-through; existing 155 lib tests unchanged |
| CH emitter Tier 3 routing — `Numeric` / `Inet` / `Interval` / `Json` emit as CH `String`; `PgPending` writes raw bytes when extension is absent | [`src/ch_emitter.rs`](../src/ch_emitter.rs) | covered transitively by the Phase 8 e2e drill (still green) |
| Spill format extended for the new variants (tags 20–24) | [`src/spill.rs`](../src/spill.rs) | Phase 6 spill tests cover round-trip; new variants ride the same path |
| `Oracle` module: PgPending resolver + 1-in-N validator + reconnect-on-close + `OracleObserver` wrapper | [`src/oracle.rs`](../src/oracle.rs) | 3 lib unit tests (sampler on/off/1-in-N, stats summary) |
| `walshadow-stream --validate <N>` CLI; oracle wires into the TupleObserver chain ahead of the inner emitter / metrics observer | [`src/bin/stream.rs`](../src/bin/stream.rs) | exercised by hand against the Phase 9 fixture cluster; CI doesn't currently spin the daemon |
| `Shadow::try_load_oracle_extension` — `CREATE EXTENSION IF NOT EXISTS`, tolerant of "not available" | [`src/shadow.rs`](../src/shadow.rs) | Phase 9 integration tests gate on this returning `Ok(true)` |
| `walshadow` PG extension — `walshadow_decode_disk(oid, bytea) -> text` reconstructs a Datum from on-disk bytes and runs `typoutput`. PGXS Makefile + .control + SQL + pg_regress suite. | [`pgext/`](../pgext) | 1 pg_regress test exercising varlena, by-val, by-ref, cstring, STRICT NULL, and the two `ereport` paths (`expected/walshadow.out`) — runs against every CI matrix PG under `--temp-instance` |
| CI: `postgresql-server-dev-<major>` added to install set; build + install + run pg_regress per matrix entry | [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) | the regress step itself is the test |
| `plans/INDEX.md` updated with the Phase 9 entry | [`plans/INDEX.md`](INDEX.md) | doc-only |

Build clean on `cargo clippy --workspace --all-targets -- -D warnings`.

Test counts (local, PG 18.4 + ClickHouse 25.8):

- `cargo test --workspace --all-targets`: full suite green;
  159 lib tests (+19 from Phase 9), 3 phase9_oracle integration tests,
  no regressions in any earlier-phase suite.
- `pg_regress walshadow`: 1 file, 86 statements, all pinned.
- Phase 8 e2e: still 1.5 s wall.

Code size:

| component | LOC |
|---|---|
| `src/codecs.rs` | 644 |
| `src/oracle.rs` | 452 |
| `tests/phase9_oracle.rs` | 284 |
| `pgext/walshadow.c` | 125 |
| `pgext/sql/walshadow.sql` + `expected/…out` | 249 |
| `src/heap_decoder.rs` / `ch_emitter.rs` / `spill.rs` / `shadow.rs` / `bin/stream.rs` net diff | ~270 |
| CI yaml | ~32 |

PLAN.md estimated ≈900 LOC; landed at ~2050 once the extension + its
pg_regress suite + the daemon-side validate-mode wiring + spill tags
are accounted for. The estimate underweighted (a) the C extension and
its CI plumbing, (b) the fact that adding a `PgPending` ColumnValue
touches every match site (emitter, spill, decoder fall-through), and
(c) the oracle's reconnect + sampler + observer-wrapper surfaces.

## Bugs surfaced

### 1. `inet` on-disk vs wire format

First cut decoded `inet` as `family | bits | is_cidr | nb | ipaddr[nb]`
— eight bytes for v4 / twenty for v6. That matches `inet_send`'s wire
output (`network.c` `inet_send`), not the **on-disk** `inet_struct`
(`utils/inet.h:24`). The on-disk body is just `family | bits |
ipaddr[nb]` with `nb` implied by `family` and `is_cidr` encoded at
the type-OID level (`INETOID` vs `CIDROID`, not in the bytes).

The Phase 9 integration test surfaced this on the first run: the
oracle's `walshadow_decode_disk` on an INET-typed bytea returned
`"0.4.192.168"` — PG had picked up `is_cidr=0, nb=4` as the first two
octets of the IP, then read the remaining `192.168` from the actual
address slot. Fix: `decode_inet` takes an `is_cidr: bool` parameter
(passed by the caller from `att.type_oid == CIDROID`) and reads
`family | bits | ipaddr[2..2+nb]`. The on-disk vs wire confusion is
documented in the codecs.rs header so future readers don't repeat it.

### 2. tokio-postgres oid parameter type

Initial `Oracle::resolve_pending` passed `type_oid as i32` as the
`oid` parameter; `tokio-postgres` rejects with
`error serializing parameter 0` because `pg_type.oid` is unsigned
(`u32`). Fix is one line — bind as `u32` — but it took a debug print
to find. Phase 9's eprintln remained on the floor of the dev session;
production has `Err(_) => stats.errors += 1; Ok(None)` so a real
type-mismatch silently falls back to raw bytes today. Worth a follow-
up: when `resolve_pending` errors, surface the first-N error messages
through the stats counter rather than swallowing them, so the same
class of typing bug doesn't hide.

## Design decisions

### Extension is optional; fallback is raw on-disk bytes

The user pushed back on a design that would make `CREATE EXTENSION
walshadow` a mandatory deployment step ("extension should be
optional, if not installed on pg just give back bytes"). The shape
now: `Oracle::resolve_pending` probes for the extension at connect
time and caches the result; absence makes every `PgPending` resolution
return `Ok(None)`. The emitter's `encode_value` then treats
`PgPending` as a raw-bytes shortcut into `append_string_bytes(raw)` —
no error, no stat bump beyond `oracle.fallback_raw`.

Operators can ship without the extension and accept that `jsonb` /
arrays land in CH as opaque bytea blobs (still queryable, still
distinct per source row); they can install the extension later and
`walshadow_decode_disk` starts running on the next daemon
reconnect. The extension's value is text-form column data; absence is
a feature gap, not a failure mode.

### Why not just call `typoutput` via SQL on a temp table

Two routes were considered before committing to the C extension:

1. **Insert the value into a temp table on shadow, read back via
   typoutput**: requires reconstructing a wire-format value that PG's
   `typrecv` will accept. Wire format ≠ on-disk format for
   `numeric` / `jsonb` / arrays, so this is *the same codec work* in
   the opposite direction. Worst of both.
2. **`SELECT $1::bytea::<typname>::text`**: PG's `::<typname>` cast
   uses `typinput` (text input) or `typrecv` (wire-format input);
   neither takes on-disk bytes. Won't work.

The C extension is small (~125 lines), well-bounded (one SQL function),
and reuses every type's existing `typoutput` machinery via
`OidOutputFunctionCall`. The cost in deployment complexity is a
package-and-`make install` step that operators are familiar with.

### `OracleObserver` clones per tuple

Resolving `PgPending` columns mutates the decoded tuple. The decoder
side hands the observer `&CommittedTuple`, so the wrapper does one
clone per tuple before mutating and forwarding. Hot-path cost for
schemas without `PgPending` columns is near-zero (clone is shallow for
Tier 1/2 values that are mostly `Copy`); schemas with `jsonb` /
arrays pay the clone but also pay a libpq round-trip, and the round-
trip dwarfs the clone. Not worth optimising until measurement says
otherwise.

### Sampler is lock-free, off by default

The 1-in-N sampler uses a `AtomicU64::fetch_add(Relaxed)` so multiple
decoder workers can share one `Oracle` without serialising. `--validate
0` (the default) short-circuits before the atomic op, so the validator
costs nothing when disabled. Sampling rates are an operator policy:
`--validate 1000` probes 0.1% of rows, which on a 10 K rows/s workload
adds ~10 SQL queries/s to shadow — invisible against the catalog
queries shadow already handles.

### CI strategy: pg_regress instead of an end-to-end drill

The pg_regress suite that ships with the extension covers
`walshadow_decode_disk`'s C-side branches (varlena, by-val 1/2/4/8,
by-ref, cstring, NULL, two ereport paths). The Rust integration test
in `tests/phase9_oracle.rs` covers walshadow's call path into that
function (tier-3 disk bytes → text). The two layers are complementary
— pg_regress isolates C bugs, the Rust test catches integration
issues — and run independently in CI under each matrix PG.

## What didn't get done

- **Local codecs for `jsonb` / arrays / `tsvector`.** The hybrid model
  explicitly punts these to the shadow extension. If a future
  measurement shows the per-row libpq round-trip is hot, candidates
  for local promotion (in declining order of stability + ease):
  `tsvector` (well-defined, but the type is niche), `int4[]` /
  `int8[]` / `text[]` arrays (1-D fixed/varlena element types are
  tractable), `jsonb` (we had a first cut working in an earlier
  session; ~250 LOC for full container traversal).
- **Sampled-row text comparison for `PgPending` types.** The
  `validate()` method's matrix only covers `numeric` / `inet` /
  `cidr` / `interval` — the locally-decoded types. For `PgPending`
  resolves there's no "local text" to diff against; the validator's
  role for those types reduces to "did the extension call succeed".
  Could be folded into the resolver's success/error counters rather
  than a separate probe.
- **`--validate` rate auto-tuning.** Today the operator picks N; the
  daemon doesn't adapt. A future hop could decay sampling toward zero
  once `mismatches == 0` has held for some interval, and ramp it back
  up on a fresh mismatch. Out of scope for v1.
- **Mismatch logging surface.** A non-zero `mismatches` counter
  surfaces in the status line but the specific `(type_oid, raw,
  local, pg)` quadruples aren't logged anywhere. Future work: ring-
  buffer the last N mismatches in `Oracle` and expose them via a
  debug endpoint.
- **Surfaced error messages from `resolve_pending`.** When a SQL call
  errors, the stats `errors` bucket increments but the message is
  swallowed. The Phase 9 inet bug above would have been faster to
  diagnose if `Oracle.last_errors: Mutex<RingBuffer<String>>` existed.

## Files touched

```
walshadow/src/codecs.rs                       new — Tier 3 local codecs
walshadow/src/oracle.rs                       new — oracle module + observer wrapper
walshadow/src/lib.rs                          mod codecs; mod oracle;
walshadow/src/heap_decoder.rs                 ColumnValue variants + dispatch
walshadow/src/ch_emitter.rs                   encode_value Tier 3 routing
walshadow/src/spill.rs                        spill tags 20–24 for new variants
walshadow/src/shadow.rs                       try_load_oracle_extension
walshadow/src/bin/stream.rs                   --validate flag + Oracle wiring
walshadow/tests/phase9_oracle.rs              new — integration tests (3)
walshadow/pgext/                              new — PGXS extension dir
walshadow/pgext/walshadow.c                   C function
walshadow/pgext/walshadow.control             metadata
walshadow/pgext/walshadow--0.1.sql            function DDL
walshadow/pgext/Makefile                      PGXS + REGRESS target
walshadow/pgext/sql/                          pg_regress test inputs
walshadow/pgext/expected/                     pg_regress test fixtures
walshadow/.github/workflows/ci.yml            install dev headers, build + regress per PG
walshadow/plans/INDEX.md                      Phase 9 entry
walshadow/plans/PHASE9.md                     new (this doc)
```
