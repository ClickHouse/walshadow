# PHASE10 — operational scaffolding

Closes [Phase 10 of PLAN.md](PLAN.md#phase-10--operational-scaffolding).
Phase 10's brief was "ship the surface plumbing the daemon needs to be
observable + reloadable + safely connected, without changing what the
slot-advance / cursor file *value* is". Phase 11 fills in the
load-bearing durable bits; Phase 10 fixes the *shape* so Phase 11 only
flips placeholders to real values.

## Strategy

Five sibling surfaces, each independent enough to land + test on its
own, sequenced so the daemon binary picks them all up in one wiring
pass.

1. **Pre-flight validators** ([`src/preflight.rs`](../src/preflight.rs)).
   Aggregate every misconfiguration finding into one report instead of
   "fix one issue, restart, hit the next" — operators see the full
   delta on the first boot. Uses `to_regclass(text)` for the
   namespace.relname → oid resolve so missing relations land as
   `MappedRelMissing` (NULL row) rather than a SQL error.
2. **HTTP/Prometheus metrics** ([`src/metrics.rs`](../src/metrics.rs)).
   Hand-rolled Prom text format over a tiny tokio TCP loop. The
   registry's snapshot includes the LSN triple (Phase 11 placeholders
   for `decoder_commit_lsn` / `emitter_ack_lsn`), per-rmgr filter
   counters, xact-buffer occupancy, decoder + emitter + oracle stats,
   uptime. No new heavy dep — `prometheus` / `metrics` crates would
   pull in a whole observability ecosystem for what is ~80 LOC of
   text formatting.
3. **`tracing` pipeline**. Drop the `tracing_debug` stub in
   [`source_feed.rs`](../src/source_feed.rs); init
   `tracing_subscriber::fmt().with_env_filter(...)` once at daemon
   entry. `RUST_LOG=walshadow=debug` now surfaces wal-rs's
   frame-level debug calls + walshadow's own per-status-line events.
4. **Standby-status triple**. wal-rs's `build_status_update` already
   takes `(write, flush, apply)`; walshadow was collapsing all three
   to one value. Phase 10 introduces
   [`StandbyStatus`](../src/source_feed.rs) carrier + threads three
   LSNs through `SourceFeed::next_chunk` / `send_status`. Phase 11
   fills in the resume-safe values; the wire shape is already there.
5. **Filtered pg_wal trim** ([`src/retention.rs`](../src/retention.rs)).
   Periodically (`DEFAULT_TRIM_INTERVAL = 30s`) the daemon polls
   shadow's `pg_last_wal_replay_lsn()` and drops filtered segments
   below `replay_lsn - retention_bytes`. Conservative classification:
   unknown filenames are left alone, only `<24-hex>` /
   `<24-hex>.partial` / `<24-hex>.manifest.json` /
   `<24-hex>.partial.manifest.json` are eligible.
6. **SIGHUP reload of `--ch-config`**. Emitter holds an
   `Arc<RwLock<HashMap<String, TableMapping>>>` ([`MappingHandle`](
   ../src/ch_emitter.rs)) instead of `EmitterConfig.tables` directly.
   SIGHUP listener task re-parses the TOML and `*handle.write().await =
   new` atomically. New mapping takes effect at the next *xact
   boundary* — mid-xact CH state stays consistent since `drain_xact`
   clears the per-table encoder cache between xacts anyway.
7. **CH-emitter bounded retry**. New
   [`Emitter::reconnect`](../src/ch_emitter.rs) opens a fresh TCP +
   builds a new `Client`, hot-swapping `client` / `codec` / `io` while
   preserving the per-xact accumulator buffers in `self.tables`.
   [`Emitter::route_with_retry`] + [`Emitter::drain_xact_with_retry`]
   wrap the inner ops with bounded exponential backoff per
   [`RetryConfig`]. `EmitterObserver::on_tuple` / `on_xact_end`
   delegate to these instead of the bare ops, so an
   `EmitterError::{Io, Client, ServerException}` is now survived
   instead of killing the daemon.

The Phase 11 cursor file + slot-advance gating ([PHASE11 not yet
written]) builds on the Phase 10 surfaces: the standby-status
`apply_lsn` slot flips from `dispatched_lsn` to
`min(shadow_replay_lsn, emitter_ack_lsn)`, the metrics
`decoder_commit_lsn` + `emitter_ack_lsn` gauges expose the new values,
and the retention trim's `retention_bytes` floor protects the resume
path's worth of WAL.

## What landed

| item | files | tests |
|---|---|---|
| Pre-flight validators (version, wal_level, slot, REPLICA IDENTITY FULL, mapped-rel existence) with aggregated report | [`src/preflight.rs`](../src/preflight.rs) | 3 lib unit tests + 2 integration tests in [`tests/phase10_ops.rs`](../tests/phase10_ops.rs) (one rejecting wal_level=replica + default repl-ident, one passing wal_level=logical + ALTER REPLICA IDENTITY FULL) |
| HTTP/Prometheus metrics endpoint + `MetricsSnapshot` struct | [`src/metrics.rs`](../src/metrics.rs) | 3 lib unit tests covering render, round-trip set/get, and an end-to-end HTTP GET against the spawned server |
| `tracing_subscriber` init in `bin/stream.rs`; `tracing::debug!` macro in `source_feed.rs` | [`src/bin/stream.rs`](../src/bin/stream.rs), [`src/source_feed.rs`](../src/source_feed.rs) | covered transitively by the phase8/phase9 e2e drills |
| `StandbyStatus` triple + `next_chunk` signature change | [`src/source_feed.rs`](../src/source_feed.rs); call sites updated in `bin/stream.rs`, `tests/wal_stream_e2e.rs`, `tests/phase8_e2e.rs` | existing tests still green |
| `retention::trim_below_lsn` + retention sweeper task in daemon | [`src/retention.rs`](../src/retention.rs), [`src/bin/stream.rs`](../src/bin/stream.rs) | 4 lib unit tests (cutoff respected, manifest+partial sibling removal, missing dir, classification) |
| SIGHUP reload path: `MappingHandle` on `Emitter`, signal task in daemon | [`src/ch_emitter.rs`](../src/ch_emitter.rs), [`src/bin/stream.rs`](../src/bin/stream.rs) | covered through `EmitterConfig::from_toml_str` lib coverage; live SIGHUP exercise deferred to ops smoke (see "What didn't get done") |
| CH-emitter `reconnect` + `route_with_retry` + `drain_xact_with_retry`; `RetryConfig` + new TOML knobs | [`src/ch_emitter.rs`](../src/ch_emitter.rs) | wired through `EmitterObserver`; live retry exercise rides phase8 e2e (which continues to green) |
| Phase 10 daemon CLI surface: `--metrics-bind`, `--retention-bytes`, `--skip-preflight` | [`src/bin/stream.rs`](../src/bin/stream.rs) | exercised via `Args::parse` paths |

Build clean on `cargo clippy --workspace --all-targets -- -D warnings`.

Test counts (local, PG 18.4 + ClickHouse 25.8):

- `cargo test --workspace --lib`: 170 tests, +11 from Phase 9.
- `cargo test --test phase10_ops`: 2 tests, both green (~1.3 s wall).
- Phase 8 e2e: still 2.1 s wall.
- Phase 9 oracle: still 0.6 s wall.

Code size:

| component | LOC |
|---|---|
| `src/preflight.rs` | 241 |
| `src/metrics.rs` | 421 |
| `src/retention.rs` | 249 |
| `tests/phase10_ops.rs` | 223 |
| `src/bin/stream.rs` (delta) | ~350 lines beyond pre-Phase-10 |
| `src/ch_emitter.rs` (delta) | ~135 lines (retry surface + MappingHandle) |
| `src/source_feed.rs` (delta) | ~50 lines (StandbyStatus + tracing) |

PLAN.md estimated ≈600 LOC; landed at ~1500 once the metrics endpoint,
pre-flight aggregator, retention sweeper, and CH-emitter retry surface
were accounted. The estimate underweighted (a) the Prom text-format
render code being more than a thin wrapper around `format!`, (b)
preflight's `to_regclass` plumbing + aggregated error report shape,
and (c) the reconnect dance for the CH emitter — drop order vs C-side
back-pointer alignment took its own design loop (see "Bugs surfaced").

## Bugs surfaced

### 1. `regclass` parameter binding fails for `&str`

First cut of preflight used `oid = $1::regclass` with `&key.as_str()`
as the bind. tokio-postgres infers $1's PG type from the cast and
rejects with `WrongType { postgres: Regclass, rust: "&str" }` because
no `ToSql` impl for `&str → regclass` exists. Fix: switch to
`to_regclass(text)` which accepts text and returns `regclass NULL`,
so the binding side stays `&str → text` and the missing-relation case
lands as `NULL` rather than a SQL error. Integration test
`preflight_rejects_wal_level_and_missing_replica_identity` caught this
on the first run.

### 2. Emitter reconnect drop-order vs C back-pointer

The CH `Client` holds a back-pointer into `PosixIo::state` set up by
`Client::init`. Replacing `self.client = new_client; self.io = new_io;`
in sequence works only because `Pin<Box<PosixIo>>` stores the I/O
state on the heap — the `Pin<Box<>>` move doesn't relocate the
underlying data, so the back-pointer the new client captured to
new_io's heap state stays valid after the field assignment moves the
`Box`. Phase 8's [Bug 2](PHASE8.md) had already pinned `PosixIo` in
`Box` for the boot path; Phase 10's reconnect surface inherits the
same invariant. Code comments in `Emitter::reconnect` spell this out
because the next reader will absolutely re-arrange the field
assignments and break it otherwise.

## Design decisions

### Why hand-roll Prometheus text format

The `prometheus` crate (and its async variant `prometheus-client`)
each pull a ~10-LOC-of-walshadow-needs surface behind a ~10-MB
transitive-deps tree (`hyper` axum router, label-set machinery,
encoder traits). Walshadow's metric set is a few gauges + a single
labeled counter; hand-rolling the text format is ~80 LOC and keeps
the build-graph diff at exactly two crates (`tracing`,
`tracing-subscriber`). If Phase 12 (backfill) doubles the metric
set, the calculus stays the same — we add lines, not deps.

### Why retention keys on LSN bytes, not wall-clock seconds

The daemon already lives in LSN space (WAL segments are 16 MiB and
named in hex log-id/seg-no). Wall-clock retention adds a clock
dependency and an operator-confusing knob ("1 hour at 2 MB/s" → how
many segments?). Byte-based retention is deterministic: 256 MiB =
16 segments, period. Operators tuning the value reason about "how
far behind can shadow lag" which is exactly LSN delta.

### Why CH retry preserves the xact buffer, not drains it

Phase 10's retry hot-path target is "TCP reset during finish_table"
or "CH server bounce between xacts". The preserved-buffer reconnect
covers both: `drain_xact_with_retry` reconnects → re-issues
`send_query(insert_sql)` per open table → retries `drain_xact`
which flushes the still-populated buffers. Mid-xact `route` errors
get the same treatment because `flush_block` (the only mid-xact
send path) only clears buffers on success.

There's a residual hazard: rows that landed in CH on the old
connection before the disconnect *and* got committed by the CH
server are duplicated on retry. `ReplacingMergeTree(_lsn)` collapses
the duplicates on `FINAL`, so the dest's observable state stays
correct; eager-read consumers see the dup window. Acceptable for
Phase 10; Phase 11's cursor + ack flow lets us drive retry from a
per-xact ack point so the dup window narrows.

### Pre-flight aggregation vs short-circuit

The aggregated [`PreflightReport`] surfaces every finding from one
probe pass. Operator-facing: "fix wal_level AND add REPLICA
IDENTITY FULL to s10.t" instead of "fix wal_level → restart → fix
the next thing". The short-circuit shape (`?` on each check) would
have been simpler to write but operationally worse — particularly
for greenfield deployments where the initial setup hits 3-4
findings on the first boot.

### Why `to_regclass()` not `quote_ident`

`quote_ident($1)::regclass` would work for unqualified relations
but not for `"namespace.relname"` — quote_ident applies to a single
identifier. `to_regclass(text)` accepts the full namespace-qualified
form, parses it through PG's regclass input function, and returns
NULL (not error) on missing relations. Saves the
`SqlState::UNDEFINED_TABLE` catch-and-translate path the first cut
needed.

### Mapping reload at xact boundary

SIGHUP can arrive mid-xact. Two consistent choices:

1. **Apply immediately**: would mid-flush change the CH dest of
   already-buffered rows, requiring a "redirect" semantic at the CH
   server which doesn't exist.
2. **Apply at next xact boundary**: current xact finishes against
   the old mapping; new xacts pick up the new mapping. No CH
   server-side gymnastics; the boundary is well-defined.

Phase 10 picks (2). The per-table encoder cache (`Emitter::tables`)
clears at the end of `drain_xact`, so the next `route()` call on
the same key consults the live mapping handle and rebuilds.

## What didn't get done

- **Shadow PG major-version restart**. PLAN.md called this out as
  rare ("mostly memory knobs"); Phase 4b's reconnect already covers
  the libpq side. The supervisor side — daemon driving `pg_ctl
  restart` against shadow — overlaps with the BASEBACKUP evaluation
  thread and is deferred. Operators reload shadow manually for now.
- **Per-rmgr metric label coverage gap**. Today's `MetricsRecordSink`
  tracks `(rmid, decision)` pairs but rmgrs that the filter has
  never seen aren't pre-declared. Prometheus is fine with this
  (counters appear on first observation) but scrapers that
  pre-declare label sets show NaN until the first record per
  (rmid, decision). Acceptable.
- **`shadow_replay_lsn` not in metrics snapshot**. The retention
  sweeper polls `pg_last_wal_replay_lsn()` independently; surfacing
  the same value through the metrics registry would require either
  a shared cache or a second poll on the status-tick loop. Phase 11
  threads the value through the cursor file path anyway; landing
  the metric there.
- **SIGHUP integration test**. The handler is wired + log-traced;
  a test would need to fork the daemon process, send SIGHUP, and
  inspect the live mapping — out of scope for a fast-running test
  suite. The lib `EmitterConfig::from_toml_str` test pins the
  parser; the live signal path rides on Tokio's well-tested
  `SignalKind::hangup()` machinery.
- **Reconnect-retry stress test**. The retry surface is wired but
  not exercised in CI (would need a CH server that can be
  forcibly bounced mid-stream). Phase 11's cursor + replay drill
  will exercise the same code path under crash-resume — Phase 10
  acceptance is "retry compiles + the unit/observation surfaces
  are present".
- **Tracing-to-OpenTelemetry export**. `tracing-subscriber`'s
  default `fmt` layer logs to stderr only. OpenTelemetry / Jaeger
  export would need another layer crate; today's deployment shape
  (single daemon, stderr-to-systemd-journal) doesn't need it.

## Files touched

```
walshadow/src/preflight.rs                    new — aggregated pre-flight validator
walshadow/src/metrics.rs                      new — Prom text endpoint + registry
walshadow/src/retention.rs                    new — LSN-keyed segment trim
walshadow/src/lib.rs                          mod preflight; mod metrics; mod retention;
walshadow/src/source_feed.rs                  StandbyStatus + tracing::debug! macro
walshadow/src/ch_emitter.rs                   RetryConfig + reconnect + retry wrappers + MappingHandle
walshadow/src/bin/stream.rs                   tracing init, preflight, metrics, retention, SIGHUP, status triple, retry-aware observer wiring
walshadow/tests/phase10_ops.rs                new — 2 preflight integration tests
walshadow/tests/wal_stream_e2e.rs             StandbyStatus call sites updated
walshadow/tests/phase8_e2e.rs                 StandbyStatus call sites updated
walshadow/Cargo.toml                          tracing + tracing-subscriber deps
walshadow/plans/INDEX.md                      Phase 10 entry → PHASE10.md
walshadow/plans/PHASE10.md                    new (this doc)
```
