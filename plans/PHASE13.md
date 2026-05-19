# PHASE13 — streaming-fed shadow for consistent subsecond CDC

Today walshadow parses + filters WAL one 16 MiB segment at a time
([`src/wal_stream.rs:12-16`](../src/wal_stream.rs)) and shadow PG
consumes filtered segments via `restore_command = 'cp out/%f %p'`
([`src/shadow.rs:200`](../src/shadow.rs)). Both halves run on
segment cadence, so source UPDATE → CH FINAL latency is bimodal:
records that arrive late in a near-full segment land instantly,
records on a quiet source wait up to `archive_timeout`. The
`pg_switch_wal` shim in `docker/DEMO.md` is the operator-visible
admission of that floor

Worse, the catalog gate `wait_for_replay(at_lsn)`
([`src/shadow_catalog.rs:446`](../src/shadow_catalog.rs)) sits on
shadow's `pg_last_wal_replay_lsn()`, which advances only when
shadow's `restore_command` finds a new segment file. A descriptor
cache miss (post-DDL, post-relmap-update, post-BASE-BACKUP) therefore
stalls the decoder for an entire segment regardless of source
activity

Goal: source UPDATE → CH FINAL ≤ 1 s at p99 under mixed DML + DDL
traffic, with no `pg_switch_wal` shim, no `archive_timeout` shim,
and no operator-visible latency cliffs

## Strategy

Walshadow becomes shadow's **streaming primary**. Filtered WAL flows
record-by-record over the PG streaming-replication protocol from
walshadow's walsender to shadow's walreceiver. Shadow's replay LSN
advances at network + apply cadence (ms), so both the segment writer
and the catalog gate stop being latency floors. Filtered segments on
disk continue to land, but as the archive fallback
(`primary_conninfo` + `restore_command`, the standard PG dual-source
pattern) and durable artifact, not the hot path

Six pieces, sequenced so each lands testable on its own

### 1. Streaming filter (record-cadence parse + rewrite)

Lift `SegmentWalker`'s page-by-page state machine
([`src/segment.rs:66-109`](../src/segment.rs)) into a stateful
struct that `WalStream::push` drives as bytes arrive. State that
must survive across `push` calls:

- current page parser cursor + page magic
- pending record stitched across pages (already modelled by
  `Pending`)
- cumulative manifest entries for the current segment
- 16 MiB rewrite buffer (still accumulated, since `noop_replace`
  rewrites land here and the segment writer needs the same bytes)
- byte-range index from logical bytes → physical offsets (for
  cross-page record rewrites)

Walker yields a `(record, byte_ranges)` tuple as soon as the
record's last byte arrives. `WalStream::push` then:

1. parse → `Filter::decide`
2. if `Drop`: `noop_replace` at the recorded byte ranges (in the
   accumulating segment buffer)
3. dispatch `record_sink.on_record` (decoder)
4. dispatch `shadow_stream_sink.on_record_bytes` (shadow wire — §3)
5. append manifest entry

When the 16 MiB buffer fills, `segment_sink.on_segment` flushes the
already-filtered bytes + accumulated manifest. No re-parse, no
rewrite re-walk

This is a hard prerequisite for streaming-fed shadow: without
record-cadence rewrite, shadow's wire stays segment-cadence

### 2. Walsender server in `wal-rs`

`wal-rs/src/pg/replication/{conn,stream}.rs` covers the **client**
side (walshadow → source). Streaming-fed shadow needs the
**server** side (shadow → walshadow). Minimal surface, mirroring
the client roles:

| query | reply | notes |
|---|---|---|
| `StartupMessage` with `replication=true` | auth challenge → `ReadyForQuery` | trust auth via unix socket in container; SCRAM for production |
| `IDENTIFY_SYSTEM` | `(systemid, timeline, xlogpos, dbname)` | forward source's reply, cached at walshadow startup, refreshed on timeline switch |
| `TIMELINE_HISTORY <tli>` | history file body | walshadow caches source's history files; new upstream fetcher |
| `START_REPLICATION [SLOT _] PHYSICAL <lsn> [TIMELINE <n>]` | `CopyBothResponse` then `'w'` frames | slotless on the walshadow side (mirrors how walshadow talks to source) |
| `BASE_BACKUP` | unsupported | shadow is basebackup'd from source by the bootstrap path |
| `CREATE_REPLICATION_SLOT` / `DROP_REPLICATION_SLOT` | unsupported | shadow doesn't set `primary_slot_name` |

Frame encoding is already in [`wal-rs/src/pg/replication/stream.rs`](../wal-rs/src/pg/replication/stream.rs)
in decode form; add the encode side. Inbound `'r'` standby status
drives the per-connection `(write, flush, apply)` LSN tracking.
Bidirectional `'k'` keepalive on idle timeout. Ignore `'h'`
hot-standby-feedback frames (no source-side horizon to honour, since
walshadow holds no slot)

Listening transport: unix socket in `/var/run/postgresql/.s.PGSQL.<port>`
shared with shadow (same container). TCP on `127.0.0.1` is the
fallback for non-colocated deployments

### 3. `ShadowStreamSink`

New sink that composes alongside `DirSegmentSink` and
`BufferingDecoderSink` on `WalStream`. On each filtered record it
appends the rewritten bytes onto every active shadow connection's
send buffer, framed as `'w'` `XLogData`. On each segment boundary
it advances the per-connection `server_wal_end` for keepalives

Tracking state:

- per-connection `dispatched_to_shadow_lsn` (high water of bytes
  pushed onto the wire)
- per-connection `shadow_flush_lsn`, `shadow_apply_lsn` (from `'r'`
  status messages), exposed as a sink-level minimum across active
  connections
- listening socket lifecycle: accept new shadow connections,
  reject if walshadow is in bootstrap

Backpressure:

- socket send buffer fills → `on_record_bytes` returns `Pending`
- `WalStream` does not block source; instead it relies on the
  segment writer + retention sweep to absorb the burst. Shadow
  reconnects later and catches up via `restore_command` from
  `out/`
- Falling-behind connections get a configurable lag ceiling
  (`shadow_apply_lag_max`) past which walshadow drops the socket
  and lets shadow reconnect through the archive path

Decoder gate ordering:

- old plan ([`src/wal_stream.rs:612-617`](../src/wal_stream.rs)):
  segment sink writes first so `restore_command` can clear the gate
  before per-record dispatch
- new plan: shadow stream sink frames first, decoder dispatch
  second. The gate `wait_for_replay(record.lsn)` then polls
  shadow's apply LSN driven by the wire, not by disk

### 4. Shadow lifecycle change

`Shadow::enable_standby_recovery` ([`src/shadow.rs:200`](../src/shadow.rs))
currently writes `standby.signal` + appends `restore_command` to
`postgresql.conf`. Extend to also append `primary_conninfo`:

```
primary_conninfo = 'host=/var/run/postgresql port=<walshadow_walsender_port> user=walshadow application_name=shadow sslmode=disable'
restore_command  = 'cp <filter_dir>/%f %p'
recovery_target_timeline = 'latest'
```

Both lines coexist by design — `primary_conninfo` is the hot path,
`restore_command` is the catchup-on-reconnect fallback. PG's
walreceiver tries `primary_conninfo` first and falls back to
`restore_command` on connect error or end-of-WAL

Bootstrap ordering constraint: walshadow's walsender listener must
be accepting connections **before** `Shadow::start` issues the
recovery-mode startup, otherwise walreceiver hits its
`wal_retrieve_retry_interval` floor and adds operator-visible lag on
first start. Add an explicit barrier in `bin/stream.rs` between
"open walsender socket" and "start shadow in recovery"

### 5. Dual-cursor durability

Two LSNs to track in the cursor file:

- `dispatched_lsn` — already exists (phase 11), advances on
  `record_sink` ack, drives CH replay-on-restart
- `shadow_flush_lsn` — new, advances on the minimum
  `flush_lsn` across active shadow connections, drives shadow
  replay-on-restart

On walshadow poison-restart:

- open new upstream connection to source at
  `min(dispatched_lsn, shadow_flush_lsn)` (source's `wal_keep_size
  = 128MB` window — if both LSNs are inside, this works; if
  shadow_flush_lsn is outside, shadow re-basebackup is needed,
  same failure mode as today's segment retention overrun)
- feed decoder pipeline starting at `dispatched_lsn` (skip earlier
  records already CH-ack'd)
- shadow reconnects on its own retry timer, asks for
  `START_REPLICATION PHYSICAL <its-flush-lsn>`, walshadow honours
  it from the upstream feed (skip earlier records on the shadow
  output path)

Two output streams, one upstream feed, independent start positions.
Idempotent under restart

### 6. Loud `ReplayTimeout`

`BufferingDecoderSink::on_record`
([`src/xact_buffer.rs:817-820`](../src/xact_buffer.rs)) swallows
`CatalogError::ReplayTimeout` as `stats.replay_timeout += 1`. Under
streaming-fed shadow the gate clears in ms in steady state, so a
timeout indicates a real fault (shadow stalled, walsender
disconnected, walshadow backed up against socket buffers). Silent
skip would shed user-heap writes invisibly

Fix: `ReplayTimeout` becomes a hard error that poisons the stream
(same contract as a sink `Err`). Daemon exits, phase 11 cursor
resumes from `dispatched_lsn` on restart. Drop the
`stats.replay_timeout` counter; replace with the poison path

## What stays (anti-goals)

- **Disk segment writes**. `DirSegmentSink` keeps writing 16 MiB
  segments to `out/`. They serve the archive fallback for shadow
  reconnect, the manifest record for retention + tooling, and the
  cold-recovery story. Sub-segment `.partial` flushes stay
  forbidden (same `on_partial_segment` contract as today)
- **Manifest emission**. One sidecar per segment, same shape.
  Per-record manifest stream would add no headroom and would break
  any `*.manifest.json` consumer
- **Retention sweep + cursor write cadence**. Both on their own
  timers, untouched
- **Decoder catalog API**. `ShadowCatalog::relation_at` and
  `wait_for_replay` keep their signatures and their semantics. The
  gate just clears fast because shadow's replay LSN advances on
  the wire
- **Bootstrap path**. `--bootstrap-mode=direct` still
  pg_basebackup's shadow from source; walshadow does not proxy
  BASE_BACKUP

## Open questions

- **Auth in the demo**. Trust auth via shared unix socket works
  for the in-container shadow. Production wants SCRAM-SHA-256 with
  a walshadow-managed credential rotation. Out of scope for phase
  13; document the gap
- **Timeline switches under load**. `XLOG_SWITCH` records must
  propagate; walshadow must invalidate its cached
  `IDENTIFY_SYSTEM` reply and `TIMELINE_HISTORY` files on timeline
  bump and fetch fresh. Source's source-of-truth check:
  `pg_stat_replication` after a promotion under traffic
- **`hot_standby_feedback`**. Shadow may emit `'h'` frames if
  configured. Walshadow has no source-side slot to advance, so
  it has no horizon to propagate. Decision: ignore the message,
  document that walshadow does not forward HS-feedback upstream.
  Long-running shadow queries that conflict with replay will hit
  the standard `max_standby_streaming_delay` resolution
- **Operator-visible lag metrics**. Add
  `walshadow_shadow_apply_lag_bytes` and
  `walshadow_shadow_apply_lag_seconds` to the metrics endpoint.
  Status-line summary gets `shadow_apply=<lsn>` next to
  `dispatched=<lsn>`. Diverging values are the operator's signal
  that shadow has fallen onto the archive path
- **Shadow apply ahead of decoder on cache miss**. With streaming,
  shadow's apply LSN can race past `dispatched_lsn` (shadow has no
  reason to wait for the decoder). Cache lookups on records past
  `shadow_apply_lsn` clear instantly. Cache lookups on records
  **behind** `shadow_apply_lsn` also clear (replay is monotonic).
  Need to verify no path queries shadow for a descriptor at a
  future LSN — `relation_at` only ever gates on `at_lsn <=
  current decoder LSN`, so this should hold by construction.
  Worth an explicit test

## Acceptance

- `UPDATE demo.users SET ... WHERE id=1` on the docker-compose
  demo source surfaces in CH within ≤ 1 s p99, with no
  `pg_switch_wal` and no `archive_timeout` shim
- `ALTER TABLE demo.users ADD COLUMN ...` followed by an UPDATE on
  the new schema also lands ≤ 1 s p99, exercising the
  post-DDL cache-miss path that the old plan left segment-cadence
- `cargo test --workspace --lib` stays green; new unit tests cover
  the streaming walker (multi-page records, page-straddling
  headers, zero-padded segment tail) and the walsender server
  (StartupMessage, IDENTIFY_SYSTEM forwarding, START_REPLICATION
  resume from arbitrary LSN, keepalive timeout)
- New integration test: kill walshadow mid-stream, restart, verify
  shadow reconnects via streaming or via `restore_command`
  depending on cursor distance from shadow's flush LSN. Both paths
  rejoin without duplicated records in CH
- Existing `phase11_cursor` integration test stays green —
  `dispatched_lsn` durability ceiling unchanged
- Existing `replay_timeout` stat removed; daemon poisons + exits
  on catalog gate timeout

## Appendix — deferred: catalog cache "rfn-may-be-stale" predicate

The previous PHASE13 plan (see git history for the pre-rewrite
revision of this file) proposed a fast-path / slow-path split in
`ShadowCatalog::relation_at`:

- **fast path**: cached descriptor for `rfn` with matching
  `generation` AND no relmap update / pg_class write to that rfn's
  database since the last `wait_for_replay` → return cached desc
  without touching the gate
- **slow path**: fall back to `wait_for_replay`. Records that hit
  this path saw segment-cadence latency

That plan was a partial optimization. It cut steady-state UPDATE
latency on warm-cache rows, but every cache miss (post-DDL, post
basebackup, post relmap update) fell off a segment-cadence cliff,
producing the operator-visible latency edges this phase exists to
eliminate

Streaming-fed shadow makes the slow path fast (ms, not 30s).
Cache miss is now affordable, so the conservative "always
invalidate on any relmap update or pg_class write" predicate is
already correct + cheap. The per-rfn / per-database tracker
accounting that the old §2 required becomes unnecessary

Defer the fast-path predicate as a future optimization, justified
only if even ms-cadence gate clearance becomes a measurable
bottleneck in a downstream consumer
