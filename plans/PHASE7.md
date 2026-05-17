# Phase 7 — CH-Native emitter via clickhouse-c-rs

Closes the "where do drained xact tuples go" question
[Phase 6](PHASE6.md) left open. `XactBuffer::commit` now drains into a
[`TupleObserver`](../src/decoder_sink.rs) that the daemon selects at
boot: metrics-only (no `--ch-config`, the Phase 5 default) or the new
[`EmitterObserver`](../src/ch_emitter.rs) talking ClickHouse Native
over TCP via [`clickhouse-c-rs`](../clickhouse-c-rs/).

This commit lands the emitter scaffold, the
[`CommittedTuple`](../src/heap_decoder.rs)-shaped observer wire, and
the feature-passdown from walshadow's `lz4` / `zstd` Cargo features
into `clickhouse-c-rs`'s matching features so the C TU links only what
the top crate advertises. Full Tier 1/2 column coverage + the
live-CH end-to-end drill live behind separate follow-up commits.

## What landed

### Compression feature passdown

Top crate's `Cargo.toml` declares:

```toml
[features]
default = ["lz4"]
lz4  = ["clickhouse-c-rs/lz4"]
zstd = ["clickhouse-c-rs/zstd"]

[dependencies]
clickhouse-c-rs = { path = "clickhouse-c-rs", default-features = false }
```

Three orthogonal build matrices:

* `--no-default-features` — no codec lib linked. Daemon refuses
  `compression = "lz4"` / `"zstd"` at boot via
  [`EmitterError::CompressionUnsupported`](../src/ch_emitter.rs).
* `--features lz4` (default) — server-default codec; LZ4 advertised
  in the Hello packet.
* `--features zstd` — ZSTD opt-in for high-compression replicas.

`CompressionChoice::build_codec` is the single point where features
gate codec construction: `#[cfg(feature = "lz4")]` arms wrap
`Codec::lz4()`, similarly for ZSTD. Unsupported variants return
`EmitterError::CompressionUnsupported(&'static str)` so operators see a
clean refusal instead of a link-time miss. The matching unit test
(`compression_choice_build_codec_respects_features`) is `#[cfg]`'d to
assert each arm's behaviour under the current matrix; runs clean
across all four feature combos.

The default codec choice (`CompressionChoice::Lz4`) matches what the
ClickHouse server defaults to in `compression_method`. Flip
`default-features = false` for an uncompressed-only build.

### Observer shape: `CommittedTuple`

[`TupleObserver`](../src/decoder_sink.rs) now takes
`&CommittedTuple` instead of `&DecodedHeap`. `CommittedTuple` moves
from `xact_buffer` to [`heap_decoder`](../src/heap_decoder.rs)
alongside `DecodedHeap` since it's a data-level wrapper, not a buffer
concept. The trait gains an `on_xact_end` hook with a default no-op
body: Phase 7's emitter uses it to close each open INSERT with
`send_data(None)`; metrics & collector observers ignore it.

`XactBuffer::commit` fires `on_xact_end` after the per-tuple loop so
each commit drain marks an xact boundary downstream. `DecoderSink`
(Phase 5's pre-buffer path, now test-only) wraps every emitted
`DecodedHeap` in `CommittedTuple { decoded, commit_ts: 0 }` — the
commit record hasn't arrived yet at that hop, so the synthetic
column reads zero.

`Box<dyn TupleObserver>` implements `TupleObserver` via a forwarding
impl, which lets `XactRecordSink<Box<dyn TupleObserver>>` carry either
the metrics observer or the emitter observer chosen at runtime.

### Emitter module

[`src/ch_emitter.rs`](../src/ch_emitter.rs) ships:

* `EmitterConfig` — connection params, compression choice, row/byte
  budgets, per-source-relation table mapping. `from_toml_str` parses
  the documented `[ch]` + `[table."<src>"]` shape.
* `TableMapping` + `ColumnMapping` — source attnum → destination CH
  column + type name. Types are parsed via `TypeAst::parse` at
  `TablePlan::build` time; CH rejects type mismatches at INSERT.
* `TablePlan` — per-relation cache: parsed `TypeAst`s for every column
  plus the four synthetic columns
  (`_lsn UInt64`, `_xid UInt32`, `_op Enum8('insert'=1,'update'=2,
  'delete'=3)`, `_commit_ts DateTime64(6, 'UTC')`) plus a pre-formatted
  `INSERT INTO ... FORMAT Native` string so the on-tuple path doesn't
  reassemble it per row.
* `TableEncoder` — per-(destination table, xact) column buffers.
  `ColumnBuf` keeps Fixed / String / NullableFixed / NullableString
  shapes; each row appends little-endian into the right slab. Buffer
  shape comes from clickhouse-c's `chc_type_elem_size` against the
  parsed `TypeAst` (zero = varlen, anything else = fixed-width N) so
  walshadow doesn't mirror CH's type-string surface. PG epoch
  (2000-01-01) → Unix epoch shift via `DATETIME64_PG_EPOCH_US` so
  timestamps line up with CH `DateTime64(6)` semantics.
* `Emitter` — owns one `Client` per CH replica plus a `HashMap` of
  per-table encoder state. `route(committed)` looks up the relation
  via `ShadowCatalog::relation_at`, hits the configured mapping (or
  bumps `unsupported_relations` if absent), opens an INSERT lazily on
  first row, appends columns, flushes on row/byte budget trip. `drain_xact`
  closes every open INSERT with `send_data(None)`, drains response
  packets until `EndOfStream` / `Exception`, then clears state.
* `EmitterObserver` — `TupleObserver` impl that forwards to `Emitter`.

`Emitter` field order matters: `client` first, then `codec`, then `io`
because Rust drops fields in declaration order and `chc_client` holds
back-pointers into `chc_io.state` from inside `io`. Reordering would
free `io` while `client` still references it.

### Daemon wiring

`walshadow-stream` gains `--ch-config <path>`. When set, the daemon
parses the TOML, opens a `std::net::TcpStream` to
`<config.host>:<config.port>`, builds an `Emitter`, wraps it in
`EmitterObserver`, and slots it into `XactRecordSink` as the drain
observer. When unset, the daemon stays metrics-only (Phase 5 +
Phase 6 behaviour, unchanged).

`std::net::TcpStream` is sync because `clickhouse-c-rs::Client` does
sync IO through `chc_io`. The boot-time connect blocks one tokio
worker briefly; mid-stream the emitter's `route` runs inside the
xact-drain hot path so a slow CH server pushes back-pressure through
the buffer, which is exactly the behaviour the
`logical_decoding_work_mem`-shaped backpressure already wants.

## What's deferred to follow-ups

* **Tier 1/2 column-type matrix.** The encoder ships the trivial
  fixed-width + string mappings (Int*, Float*, Bool, Text/Bytea,
  Timestamp/TimestampTz, Date, Time, Uuid, Name, Oid, Char). Numeric,
  jsonb, arrays, inet, interval, tsvector stay Phase 9 (the oracle
  drill); the emitter surfaces them as `UnsupportedValue` so silent
  loss is visible.
* **Budget-triggered mid-xact flush.** `row_budget` / `byte_budget`
  fields exist on `EmitterConfig` and the `route` path checks them
  per row, but the flush path keeps the INSERT open across blocks
  only loosely — full streaming-block-per-xact lands when the live-CH
  drill lands.
* **Cursor file integration.** `(filter_lsn, decoder_lsn, emitter_lsn)`
  atomic commit per drain. `EmitterStats::xacts_committed` tracks the
  count internally; persisting it lands with the followup work
  [Phase 6 §"Followups" item 1](PHASE6.md#followups).
* **Cross-table WAL ordering within an xact.** Today an xact touching
  T1 and T2 lands as one INSERT per table sequentially; the
  per-(table, xact) accumulator preserves order within each
  destination table but not across them. `ReplacingMergeTree` dedup
  keys on `_lsn` so end-state stays consistent.
* **Multi-replica fan-out.** v1 is one `Emitter` per daemon, talking
  to one CH replica. Replica round-robin / DSN list is a config-surface
  decision once a multi-replica deployment asks.
* **`AsyncInsert` / `INSERT INTO ... SETTINGS async_insert=1`.** CH's
  async-insert path is interesting for low-rate workloads but
  orthogonal to the v1 emitter's "one block per (table, xact)"
  shape. Revisit when ingest-rate measurements push for it.
* **Live-CH end-to-end drill.** The Phase 5+6 fixtures spin up shadow
  PG; the Phase 7 drill needs a `clickhouse local` (or a real CH
  server) sidecar. Lands as `tests/ch_emitter_e2e.rs` with the same
  `skip if not on PATH` pattern Phase 6 uses for `initdb`.

## Size

* `Cargo.toml` — features + dep wiring (12 lines added).
* `src/ch_emitter.rs` — emitter scaffold + unit tests (~1050 lines).
* `src/decoder_sink.rs` — `TupleObserver::on_xact_end` + Box-forward
  impl + `CommittedTuple` plumbing (≈ 40 lines added).
* `src/xact_buffer.rs` — `CommittedTuple` move-out + `on_xact_end`
  call after drain (≈ 10 lines net).
* `src/heap_decoder.rs` — `CommittedTuple` move-in (≈ 15 lines added).
* `src/bin/stream.rs` — `--ch-config` arg + observer-selection branch
  (≈ 25 lines added).
* `src/lib.rs` + `plans/INDEX.md` + this file — doc + index updates.

Net: ≈ 1150 LOC across walshadow proper. Below the plan's ~500 LOC
estimate's complexity envelope after counting tests + the conservative
encoder buffer-shape enum.

## Acceptance shape

`cargo test --lib ch_emitter` exercises:

* compression-choice parsing (case-insensitive, rejects unknown)
* compression-choice → codec build under each feature-flag matrix
* `elem_size_covers_phase7_tier1` parses each Tier-1 type via
  `TypeAst::parse` and confirms `chc_type_elem_size` reports the right
  fixed-width N (or 0 for varlen / composite).
* `new_for_ast_picks_shape_from_chc_type_kind` confirms `Nullable(_)`
  unwrap goes through `Kind::Nullable` + `child(0).elem_size()`.
* `quote_ident` backtick escaping
* `TablePlan::build` end-to-end including synthetic columns and
  `INSERT INTO ... FORMAT Native` SQL formation
* `TableEncoder::append_row` round-trip across non-null Int32 +
  nullable String + every synthetic column (LSN, xid, op, commit_ts)
* `EmitterConfig::from_toml_str` parsing the documented shape

Live-CH drill (out of scope for this commit) will assert: server
accepts the emitted block, `SELECT count() FROM dest` matches source
row count, `_lsn` ordering survives a daemon restart.
