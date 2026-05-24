# ops

Operational scaffolding for production deployment. Four sibling
surfaces: preflight validators, HTTP/Prom metrics, filtered-segment
retention, durable cursor file. None of them touches decoder fidelity; their job is to
make a long-running daemon survivable, observable, and resumable.

## Purpose

- Validate environment at boot so misconfiguration surfaces as one
  aggregated report instead of "fix one issue, restart, hit next"
- Expose every load-bearing LSN + per-rmgr filter counter + xact buffer
  occupancy as Prom text so operators see lag before CH does
- Retain filtered segments below shadow's replay head for a
  configurable debug window, drop older ones to bound disk
- Persist resume state (six LSNs + CRC) across `kill -9` so daemon
  restart hands source's slot a byte-identical write/flush/apply
  triple, and `cargo test --test kill_restart` proves end-state
  parity over 15 seeded kill/restart cycles

## Preflight validators

[`src/preflight.rs`](../src/preflight.rs). Aggregates every finding
into one [`PreflightReport`] before failing, so greenfield deployments
hitting 3-4 setup issues see them all at once. Checks driven from
`Inputs { source_sql, shadow_sql, slot, ch_config }`:

- `server_version_num >= MIN_SERVER_VERSION_NUM` (160_000). Catalog
  accessors assume PG-16 column layouts
- `source_major == shadow_major`. Same-physical-WAL standby cannot
  span major versions, PG's catalog layout diverges across them
- `wal_level = 'logical'`. Physical-only WAL omits old-tuple bytes
  UPDATE / DELETE need
- `--slot` resolves in `pg_replication_slots` when set
- Every `--ch-config` mapped relation has `relreplident = 'f'`. Uses
  `to_regclass(text)` so missing relations land as `MappedRelMissing`
  (NULL row), not a `SqlState::UNDEFINED_TABLE` SQL error. `quote_ident`
  alternative rejected, doesn't handle `"namespace.relname"` form

Each finding renders to a precise variant with operator-actionable
text (`ALTER TABLE foo REPLICA IDENTITY FULL on the source`). No
silent-skip fall-throughs. `--skip-preflight` exists for development
work, not production. Daemon prints the report and exits non-zero on
any finding.

## Metrics endpoint

[`src/metrics.rs`](../src/metrics.rs). Hand-rolled Prometheus text
format over a tokio TCP loop. No `prometheus` crate dep, ~80 LOC of
`writeln!` against an `MetricsSnapshot`. `--metrics-bind 127.0.0.1:PORT`
opens the listener; `:0` picks an ephemeral port. The HTTP server
returns the same body for any path so `curl http://host:port/` works
alongside `/metrics`.

Endpoint doubles as a bootstrap-readiness gate: integration tests
poll `fx::wait_for_listen(metrics_addr)` to detect that preflight +
bootstrap finished before driving workload.

Inventory by category:

### LSN gauges

- `walshadow_source_received_lsn` — highest `server_wal_end` on the
  replication socket
- `walshadow_filter_lsn` — last segment-boundary the filter dispatched
- `walshadow_shadow_replay_lsn` — `pg_last_wal_replay_lsn()`, polled
  shared with retention sweeper via one `Arc<AtomicU64>`
- `walshadow_decoder_commit_lsn` — wired to `XactBufferStats.drain_lsn`
- `walshadow_emitter_ack_lsn` — `XactBufferStats.emitter_ack_lsn`,
  single source of truth (see [xact.md](xact.md))

### Counters

- `walshadow_filter_records_total{rmgr,decision}` — labelled per
  (rmgr name, "keep"|"drop")
- `walshadow_xacts_{committed,aborted}_total`
- `walshadow_decoder_{decoded,partial,toast_chunks,toast_malformed}_total`
- `walshadow_emitter_{rows,blocks,xacts,unsupported_relations}_total`
- `walshadow_decode_{resolved,fallback_raw,validate_sampled,validate_mismatches,errors}_total`
  (oracle path, see [shadow.md](shadow.md))
- `walshadow_spill_evictions_total`, `walshadow_uptime_seconds`

### Buffer gauges

`walshadow_xact_{active,bytes_in_memory}`,
`walshadow_spill_{xacts_active,bytes_active}`

### Shadow apply lag

- `walshadow_shadow_apply_lag_bytes` (gauge):
  `source_received_lsn - min_apply_lsn` across active shadow
  walreceivers. Caller saturates to 0 when shadow is ahead. When no
  shadow is connected, caller passes `source_received_lsn` so
  disconnect surfaces as max lag, not silently absent
- `walshadow_shadow_apply_lag_seconds` (gauge): bytes divided by
  rolling 30 s rate estimate. `+Inf` when denominator is unknown (Prom
  convention for "no data point"); zero when lag is zero
- `walshadow_shadow_stream_active_connections` (gauge): count of
  attached walreceivers
- `walshadow_shadow_stream_dropped_connections_total` (counter):
  bumped when `ShadowStreamState::cutoff_slow_connections` drops a
  socket past `slow_threshold`. `!c.closing` gate guards against
  double-count

Render emits `# HELP` + `# TYPE` per metric. Counters use `_total`
suffix per Prom naming. `f64::INFINITY` → `+Inf`; finite floats use
`{:.3}` precision.

## RateEstimator

[`RateEstimator`](../src/metrics.rs) in `src/metrics.rs`, driven from
the status-loop tick in `src/bin/stream.rs`. 30-second rolling
`VecDeque<(Instant, source_received_lsn)>`; `observe` pushes + prunes
entries older than `window`; `rate()` returns
`(back_lsn - front_lsn) / elapsed_secs` or `None` when fewer than two
samples, zero elapsed, or zero delta.

`seconds_for(lag_bytes)` is the gauge feeder:

- `lag_bytes == 0` → `0.0`
- `rate().is_some()` → `lag_bytes / rate`
- otherwise → `f64::INFINITY` (renders as `+Inf`)

Rate window pinned at 30 s. A 5 s window swings wildly under bursty
write traffic; 60 s lags too far behind step changes. No history
persisted, restart resets.

## Tracing

`tracing_subscriber::fmt().with_env_filter(...)` initialised once at
[`bin/stream.rs`](../src/bin/stream.rs) entry. `RUST_LOG` honoured;
default `warn` + per-crate overrides. Surfaces wal-rs's frame-level
debug calls alongside walshadow's own status-line events.

Status line per tick includes `shadow_apply=<lsn>` alongside
`dispatched=<lsn>` + `drain_lsn=<lsn>`. Diverging pair is the
at-a-glance signal that shadow has fallen behind. CI runs set
`RUST_LOG=warn,walshadow=info`; artifact-emitting CI flips to
`walshadow::xact_buffer=trace` so stalled commits surface in the
captured stderr log without re-runs.

OpenTelemetry / Jaeger export deferred; single-daemon +
stderr-to-journal deployments don't need it.

## SIGHUP reload

[`bin/stream.rs::install_sighup`](../src/bin/stream.rs). Re-reads
`--ch-config` TOML, parses through `EmitterConfig::from_toml_str`,
atomically swaps the emitter's `Arc<RwLock<HashMap<String, TableMapping>>>`
([`MappingHandle`](../src/ch_emitter.rs)) via
`*handle.write().await = new`. New mapping picks up at next xact
boundary; `Emitter::tables` per-table encoder cache clears at end of
`drain_xact` so the next `route()` call consults the live handle and
rebuilds.

Mid-xact application rejected: would change CH dest of
already-buffered rows mid-flush, requiring a CH-server-side
"redirect" semantic that doesn't exist.

SIGHUP without `--ch-config` is a no-op tap. The runtime-config-from-PG
work narrows TOML scope but doesn't remove it; cross-link
[emitter.md](emitter.md),
[future/runtime_config_from_pg.md](future/runtime_config_from_pg.md).

## Filtered segment retention

[`src/retention.rs`](../src/retention.rs). Shadow's `restore_command`
copies (not moves) every segment out of the filter's output dir; the
originals accumulate forever without intervention.

`trim_below_lsn(dir, cutoff_lsn)` walks the dir, parses each filename
through `SegmentName::parse`, and removes any segment whose end LSN
(`start_lsn + WAL_SEG_SIZE`) sits at or below `cutoff_lsn`. Segment
containing `cutoff_lsn` is preserved (shadow may still be reading it).
`.partial` files (crash residue) and `.manifest.json` sidecars
(plus `.partial.manifest.json`) are removed alongside their segment.
Unknown files are left alone — trimmer is conservative on purpose so
a sibling system writing into the same dir doesn't lose unrelated
files.

Sweeper task in `bin/stream.rs` ticks every
`DEFAULT_TRIM_INTERVAL = 30s`, polls
`SELECT pg_last_wal_replay_lsn()::text` from shadow, computes
`cutoff_lsn = replay_lsn.saturating_sub(retention_bytes)`. Single
`Arc<AtomicU64>` shared with the status loop so the metrics gauge +
the sweeper see the same value with one round-trip.

`--retention-bytes` default `DEFAULT_RETENTION_BYTES = 256 MiB`
(~16 × 16 MiB segments). `--retention-bytes 0` disables the sweeper
outright. Bytes (not seconds) because daemon lives in LSN space:
"how far behind can shadow lag" is exactly LSN delta. Operator
tuning "1h at 2 MB/s" maps to bytes once.

`TrimReport { segments_removed, manifests_removed, partials_removed,
bytes_freed }` surfaces at the status line.

## Standby-status triple

[`StandbyStatus { write, flush, apply }`](../src/source_feed.rs)
threads through `SourceFeed::next_chunk`/`send_status`:

- `write_lsn = source_received_lsn` — last `server_wal_end` seen
- `flush_lsn = filter_durable_lsn` — last segment fsynced via
  `DirSegmentSink::on_segment`'s `OpenOptions+write_all+sync_all+
  rename+dir_sync` chain. Equal to `dispatched_lsn` once segment fsync
  made equivalence honest
- `apply_lsn = min(shadow_replay_lsn, emitter_ack_lsn)`. Neither side
  may advance past either replica. Carve-out: treat
  `shadow_replay_lsn == 0` as "no constraint from shadow" (sweeper
  hasn't reported, or `--retention-bytes 0`) and use
  `emitter_ack_lsn` alone — otherwise source's slot freezes at 0

Source's `pg_replication_slots.confirmed_flush_lsn` advances against
this triple; slot recycle keys on `apply_lsn`. See
[filter.md](filter.md) for `filter_durable_lsn` producer,
[shadow.md](shadow.md) for `shadow_replay_lsn` + `shadow_flush_lsn`
producers, [xact.md](xact.md) for `drain_lsn` + `emitter_ack_lsn`,
[emitter.md](emitter.md) for `on_xact_end` returning Ok being the
signal the buffer interprets to advance `emitter_ack_lsn`.

## Cursor file

[`src/cursor.rs`](../src/cursor.rs). `{spill_dir}/cursor.bin`, 64 bytes
on disk, schema version 2 (v2 added `shadow_flush_lsn` to v1 layout).

```
MAGIC "WSCRSR\x01\x00"        (8 B)
version u32 LE                (4 B)
source_received_lsn u64 LE    (8 B)
filter_durable_lsn  u64 LE    (8 B)
shadow_replay_lsn   u64 LE    (8 B)
drain_lsn           u64 LE    (8 B)
emitter_ack_lsn     u64 LE    (8 B)
shadow_flush_lsn    u64 LE    (8 B)
crc32c              u32 LE    (4 B)
```

Constants: `CURSOR_FILENAME = "cursor.bin"`,
`CURSOR_VERSION = 2`, `CURSOR_FILE_LEN = 64`, `LSN_COUNT = 6`. CRC32C
matches PG's own checksum algorithm so future "log on every checksum
failure" taps share code paths. Magic prefix doubles as
`file`/`binwalk`-friendly identifier.

Writer is `create+write_all+sync_all+rename+fsync_dir` against
`cursor.bin.tmp` → `cursor.bin`. `kill -9` between `OpenOptions::open`
+ `rename` leaves a valid-magic, valid-CRC stale `.tmp` on disk; boot
path only reads `cursor.bin` so the stale `.tmp` is ignored. Code
comment in `cursor::write` pins the invariant.

Reader (`cursor::read`) returns `Ok(None)` for greenfield (file
absent), `Ok(Some)` for a valid file, `Err(CursorError::{Size,
BadMagic, Version, Crc, Io})` for corrupt. Boot path logs the error
and falls back to greenfield resume.

`--ignore-cursor` forces greenfield boot even with a valid file on
disk. Picked over `rm cursor.bin` because `rm` between cursor write +
daemon launch races a still-running daemon; flag is atomic with the
boot sequence and leaves the prior cursor on disk for forensics.

Write cadence equals `--status-interval` (default 10 s). Per-xact
cursor write rejected on cost grounds — 1k cursors/sec worth of
disk+fsync+dir-fsync on a busy OLTP workload; PLAN §5 acceptance
doesn't require per-xact granularity.

Cursor lives at `{spill_dir}/cursor.bin` not `--cursor-path` so `mv`
of the working dir keeps spill files + cursor coherent. `cursor::cursor_path`
is the single choke point for any future `--cursor-dir` HA knob.

## Resume semantics

Boot order ([`bin/stream.rs`](../src/bin/stream.rs)):
`IDENTIFY_SYSTEM` → cursor read → resolve start LSN → preflight →
`START_REPLICATION`.

Start-LSN precedence:

1. `--start-lsn <hex>` — explicit operator override
2. `cursor.emitter_ack_lsn` (segment-aligned down) when cursor present
   and `--ignore-cursor` unset
3. Source's current write head — greenfield

Each LSN's restart role:

- `source_received_lsn`: bookkeeping only, gates nothing on restart
- `filter_durable_lsn`: highest segment fsynced on disk. Equals
  `flush_lsn` advertised to source
- `shadow_replay_lsn`: shadow PG's `pg_last_wal_replay_lsn()`,
  apply-LSN floor (when nonzero)
- `drain_lsn`: highest commit-record LSN handed to observer's
  `on_xact_end`. Strictly ≥ `emitter_ack_lsn`. Surfaces as
  `walshadow_decoder_commit_lsn`
- `emitter_ack_lsn`: highest commit-record LSN where `on_xact_end`
  returned Ok. Load-bearing resume LSN — daemon restarts here.
  Apply-LSN ceiling
- `shadow_flush_lsn`: minimum `flush_lsn` reported via inbound `'r'`
  standby status across active shadow streaming connections. On
  restart, walsender hands shadow back through
  `START_REPLICATION PHYSICAL <lsn>`. Bookkeeping-only when no
  streaming shadows are attached; on-disk `restore_command` fallback
  takes over

Apply LSN formula (`min(shadow_replay_lsn, emitter_ack_lsn)`) with
carve-out: treat `shadow_replay_lsn == 0` as no shadow constraint,
fall back to `emitter_ack_lsn` alone. Ack-only
correctness is the right trade when shadow's replay isn't on the
resume path anyway.

Dual-cursor contract for `kill -9` + restart: spill dir + cursor file
persist between kill and restart. `shadow_flush_lsn` lets the
streaming-fed shadow's resume position survive daemon bounce without
re-archiving from `out/`.

## Kill-restart drill

[`tests/kill_restart.rs`](../tests/kill_restart.rs).
Three cutoff strategies × five seeded windows = 15 daemon
spawn/kill/restart cycles per CI invocation. Source PG + CH server +
basebackup-cloned shadow stand up once, daemon cycles inside.

Strategies:

1. **mid-segment** — kill before in-flight segment reaches 16 MiB
   seal. Walshadow's cursor resumes from sub-segment LSN; streaming-
   fed shadow re-streams unsealed bytes via the wire, archive path
   catches up via partial-segment re-fetch
2. **mid-xact** — kill while at least one large xact is open (sized
   to spill via `XactBuffer` largest-first eviction; `BEGIN; INSERT
   × 10000; COMMIT` of ~250 B/row alongside the small-write loop)
3. **post-commit / pre-CH-ack** — kill the moment
   `walshadow_xacts_committed_total > 0`. Fallback shape in place of
   the originally-planned CH-side artificial-delay shim; same intent,
   simpler harness

Per-cycle: spawn daemon, wait for metrics endpoint (post-preflight
readiness gate), drive small_insert_loop, fire strategy trigger,
SIGKILL via `std::process::Child::kill()`, snapshot source's
`pg_current_wal_lsn`, restart with identical flags (same `--spill-dir`,
same `--walsender-bind`, SO_REUSEADDR on the listener), no
`--ignore-cursor`, poll `walshadow_emitter_ack_lsn` until catchup,
assert CH `count + sum(id) + md5(string_agg(name, ',' ORDER BY id))`
matches source.

`WALSHADOW_KILL_SEED` env (default `0xC11AC11A`) seeds an inline
splitmix-style LCG so CI is reproducible. Per-(strategy, run) seed
derivative shifts the 250-750 ms kill window within each strategy.
Nightly rotation across seeds surfaces rare-window bugs.

Test is NOT `#[ignore]`. Uses runtime skip-gates checking
`fx::pg_available()` / `fx::pg_basebackup_available()` /
`fx::clickhouse_available()` — silently `return` when binaries are
absent, panics on actual failure when present (switched away from
`#[ignore]` so default `cargo test` exercises the drill on any dev box
with PG + CH on PATH).

Source pins `wal_keep_size = '128MB'` so 250-750 ms of WAL stays
inside the slot-less retention window (no `--slot` set in this drill).

## pgbench acceptance drill

[`tests/pgbench_acceptance.rs`](../tests/pgbench_acceptance.rs).
v1.0 acceptance §1 end-to-end.

Pipeline: `initdb` source PG `wal_level=logical` → `pgbench -i -s 1`
(100k `pgbench_accounts`, 1 branch, 10 tellers, 0 history) →
`REPLICA IDENTITY FULL` on all four pgbench tables (preflight rejects
otherwise) → spawn CH + dest tables ReplacingMergeTree(_lsn) → spawn
`walshadow-stream --bootstrap-mode=direct
--bootstrap-autospawn-shadow` → wait for metrics endpoint
(bootstrap-finished gate, ~100k rows land via transitional emitter's
synchronous INSERTs) → `pgbench -T 6 -c 4 -j 2` background (CI uses
6 s, plan called for 30 s) → +2 s `ALTER TABLE pgbench_accounts ADD
COLUMN c int DEFAULT 7` (exercises read-time defaults via
`attmissingval`) → +4 s `CREATE INDEX CONCURRENTLY ON pgbench_history
(bid)` (catalog-cache + non-blocking-DDL exercise) → drain via
`pg_switch_wal` + `--max-segments=1`, or poll
`walshadow_emitter_ack_lsn` to source's post-switch
`pg_current_wal_lsn` → `OPTIMIZE TABLE <dest> FINAL` per table,
parity oracle on count + sum.

`c` column on CH is **`Nullable(Int32)`** not `Int32`. Bootstrap walks
heap pages where `attnum=5` doesn't yet exist (ALTER fires post-
bootstrap), and the emitter writes NULL for missing-attnum mapping
columns. Non-nullable rejects bootstrap inserts. Assertion adjusted to
(a) ≥ 1 row reaches CH with `c=7` via the read-time-default path,
(b) no row has c set to anything other than 7 or NULL. Pre-ALTER
bootstrap rows never touched by pgbench stay at `c=NULL`. See
[bootstrap.md](bootstrap.md) for the bootstrap-then-DDL column-shape
interaction.

Test NOT `#[ignore]`. Same runtime skip-gate pattern as kill-restart:
`fx::pg_available`, `fx::pg_basebackup_available`,
`fx::clickhouse_available`, plus a local `pgbench_available()`.

`--ch-flush-timeout-ms 200` holds INSERTs open across xacts; pgbench
TPC-B writes four tables/xact + per-table close is one CH EndOfStream
round-trip, so `flush_timeout=0` caps throughput at ~5 xact/s on a
local CH. 200 ms coalesces inserts into one MergeTree part per window
and lets the daemon track pgbench's ~700 xact/s.

CI matrix slot for PG 16 / 17 / 18 across same fixture — different
`postgres` binary — exists, drift surfaces as parity-check diff.

## Bounded CH-emitter retry

[`Emitter::reconnect`](../src/ch_emitter.rs) opens
a fresh TCP + builds a new `Client`, hot-swaps `client` / `codec` /
`io` while preserving per-xact accumulator buffers in `self.tables`.
[`Emitter::route_with_retry`] + [`Emitter::drain_xact_with_retry`]
wrap inner ops with bounded exponential backoff per [`RetryConfig`].
`EmitterError::{Io, Client, ServerException}` is now survived instead
of killing the daemon.

Residual hazard: rows that landed in CH on the old connection +
committed by CH server before disconnect are duplicated on retry.
`ReplacingMergeTree(_lsn)` collapses dupes on `FINAL`; eager-read
consumers see the dup window. Acceptable for v1.0.

Dual-cursor narrows the dup window, slot-advance via
`emitter_ack_lsn` means retry replays from a per-xact ack point, not
from the segment boundary. Deeper re-emit-from-spill story (replay
spill files keyed on xids whose first-seen LSN > cursor's ack) is
deferred to [future/ch_bounce_recovery.md](future/ch_bounce_recovery.md).

## Cross-links

- [filter.md](filter.md) — `filter_durable_lsn` producer
  (DirSegmentSink fsync chain)
- [shadow.md](shadow.md) — `shadow_replay_lsn` (sweeper poll) +
  `shadow_flush_lsn` (streaming) producers
- [xact.md](xact.md) — `drain_lsn` + `emitter_ack_lsn` producers
  (`XactBuffer::commit` / `abort` advance stats after `on_xact_end`
  Ok)
- [emitter.md](emitter.md) — `on_xact_end` signal interpreted by
  `XactBufferStats` advance; SIGHUP `MappingHandle` lives here
- [bootstrap.md](bootstrap.md) — cursor `start_lsn` falls back to
  bootstrap's `end_lsn` at greenfield boot; `Nullable(T)` requirement
  for post-bootstrap `ADD COLUMN`
- [future/ch_bounce_recovery.md](future/ch_bounce_recovery.md) —
  deeper re-emit-from-spill story past current retry surface
- [future/parked.md](future/parked.md) —
  `--bootstrap-autospawn-shadow` port override + the seven
  originally-`#[ignore]` tests' un-ignore drive items
- [future/runtime_config_from_pg.md](future/runtime_config_from_pg.md)
  — narrows TOML scope but doesn't remove `--ch-config` SIGHUP reload

File written.
