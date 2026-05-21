# 06 — `walshadow_shadow_apply_lag_*` metrics

Closes the PHASE13 retro carry-over
([§"What didn't land in PHASE13"](../PHASE13.md#what-didnt-land-in-phase13)).
Wire is in place — [`ShadowStreamState::aggregate`](../../src/shadow_stream.rs)
exposes `min_flush_lsn` + `min_apply_lsn` across active shadow
connections. The Prometheus surface lift is mechanical

## Why

Operator-visible signal that shadow has fallen onto the archive
path. PHASE13's streaming-fed shadow runs in two modes: primary path
is the wire (`primary_conninfo`), fallback is `restore_command`
against `out/`. A divergence between source's `pg_current_wal_lsn`
and shadow's `min_apply_lsn` past, say, a segment's worth is the
signal that the wire dropped and shadow is catching up via the
archive

No metric today means operators discover the slowdown via
end-to-end CH latency — too late, and far from the cause

## Surface

Two gauges on [`metrics.rs`](../../src/metrics.rs):

- `walshadow_shadow_apply_lag_bytes` — `source_received_lsn -
  min_apply_lsn`, raw byte count
- `walshadow_shadow_apply_lag_seconds` — same gap divided by
  estimated WAL byte rate (rolling 30 s average of source-received
  bytes / time). Drops to 0 when min_apply_lsn catches up

Plus two more for the streaming path's own state:

- `walshadow_shadow_stream_active_connections` — gauge over
  `state.connections.len()`
- `walshadow_shadow_stream_dropped_connections_total` — counter
  bumped when [`ShadowStreamState::cutoff_slow_connections`](../../src/shadow_stream.rs)
  drops a socket past `slow_threshold`

[`bin/stream.rs`](../../src/bin/stream.rs)'s status timer (currently
polls `shadow_state.lock().await.aggregate()` for the cursor write
loop) gains a sibling metrics-publish call that writes into the
shared `Metrics` snapshot the Prom endpoint reads from. Same
cadence (status-loop tick); no new task

The status-line summary
([`bin/stream.rs`](../../src/bin/stream.rs)) gains
`shadow_apply=<lsn>` next to the existing `dispatched=<lsn>`. A
diverging pair is the at-a-glance operator signal

## Tests

Unit ([`metrics.rs`](../../src/metrics.rs) test module):
- `Metrics::render` emits the four new metric names with the right
  HELP / TYPE comments
- `walshadow_shadow_apply_lag_bytes` correctly subtracts (and
  saturates at 0 when min_apply_lsn somehow exceeds
  source_received_lsn — shouldn't happen but the gauge must not
  underflow)

Integration: lift `tests/phase10_ops.rs`'s metrics-scrape assertion
to include the new four names

## Size

~80 LOC product + ~40 LOC test

## Risks

- **Byte-rate estimate stability.** A 30 s rolling window gives
  decent stability under bursty traffic; a 5 s window swings wildly.
  If the gauge churns visibly in dashboards, raise to 60 s. The
  shape is just `rate = (received_now - received_30s_ago) /
  30.0`; if there's no point 30 s old in the ring, fall back to
  "since startup" rate. No history persisted — restart resets
- **`min_apply_lsn` is `None` when no shadow is connected.** Report
  the lag gauge as `Inf` (Prometheus convention for "no data
  point") or as the gap from `source_received_lsn` to 0 (treats no
  shadow as infinitely lagged). Prefer the gap-from-zero shape so
  the dashboard alert "lag > segment_size" fires under disconnect —
  the operator wants disconnection surfaced as max-lag, not
  silently absent
