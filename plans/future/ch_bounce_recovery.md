# ch_bounce_recovery — re-emit from spill on retry-budget exhaustion

Gap #8 in the overview. Bounded retry covers the operational case;
deeper recovery needs cursor + emitter cooperation

## Lead

CH server bounces (planned restart, OOM, network partition) are the
common operational failure mode. Bounded retry inside the emitter:
each INSERT round-trip retries with backoff up to a configured budget,
daemon advances `dispatched_lsn` on success. Budget exhaustion
poisons the daemon — operator restart, cursor resumes from the last
durable `decoder_lsn`

Deeper case: budget expired, daemon died, restart picks up cursor.
Uncommitted xacts spilled to disk under
`{spill_dir}/xid-<xid>-<first_lsn>.bin` are re-decoded from there.
Already-committed-but-CH-failed xacts are gone — their xact buffer
discarded at commit time, their INSERTs never landed. Cursor sits
past their commit LSN, so re-decoding from WAL doesn't recover them
either

## Today

* Bounded retry handles transient CH-side failures (~minutes of CH
  downtime). Tested via emitter retry coverage
* Expired budget kills daemon. Cursor written at the last durable
  boundary (decoder LSN advanced only after the emitter acked or the
  xact spilled to disk + cursor fsynced)
* Restart resumes from `manifest.toml`. WAL still on source, so any xact
  whose commit LSN exceeded cursor at kill time is re-decoded clean
* `ReplacingMergeTree(_lsn)` dedups any rows the pre-kill daemon
  managed to land before the kill. End-state matches non-interrupted
  run (acceptance §5)

The hole: xacts the daemon *committed in xact-buffer terms* (cursor
advanced past their commit LSN) but never landed in CH because the
emitter exhausted its retry budget. Cursor sat past the commit but
the rows aren't in CH. On restart, decoder reads WAL post-cursor and
those rows are lost

Cursor advance rule prevents most of this: cursor advances on
`min(shadow_replay, emitter_ack)`. Emitter ack only fires on
successful INSERT, so retry-budget exhaustion holds the cursor at
the pre-failed-xact LSN. Re-decode from WAL covers it cleanly

Edge case remaining: emitter ack races. Daemon process killed
between emitter ack and cursor fsync — the ack landed, the INSERT
landed in CH, but cursor doesn't reflect it. Restart re-decodes the
xact, second INSERT lands, CH dedups via `_lsn`. Correct but
wasteful

## Sketch — read uncommitted spill files on resume, dedup against CH

Spill files for uncommitted xacts already land at
`{spill_dir}/xid-<xid>-<first_lsn>.bin`. Resume protocol could
extend that to *committed* xacts: emitter writes a spill record on
ack-failure (rather than discarding the buffer), cursor stays at
the pre-failed-xact LSN

On resume:

1. Walk spill dir, collect all `xid-*.bin` files
2. For each, re-decode → re-emit. `_lsn` dedup against CH catches
   any rows that did land pre-kill
3. Delete spill file on emitter ack

Same shape as today's uncommitted-xact resume, applied to the
"committed but failed" cohort. Reuses the spill encoder
(`encode_heap_into` / `encode_chunk_into`), cursor contract, and
emitter's retry path

## Why deferred

* `ReplacingMergeTree(_lsn)` dedup machinery in CH handles end-state
  cleanly. Operator setting `--retry-budget=infinity` (default
  generous) gets the operational case for free; the deeper recovery
  matters only if the workload demands lower-latency recovery than
  "operator restart + WAL re-read"
* WAL re-read from cursor is fast — bounded by source-side retention
  and walshadow's decode throughput, not by spill replay. A physical
  slot + the `flush_lsn = min(durable, apply_ceiling)` cap now hold
  source WAL through the CH outage (slot `restart_lsn` sticks at the
  CH-durable point), so re-read from cursor stays available unless the
  slot goes `lost` / source disk fills — that edge is fatal and recovers
  via config `initial_load` re-seed (see [../source.md](../source.md))
* Spill format extension would re-open the spill-format-version-bump
  debt; better paid once than twice

Reconsider when first operator surfaces "CH down for >hour, lost
xacts on resume". Estimated lift: ~200 LOC emitter + spill +
resume, plus integration test mirroring `kill_restart`'s
LCG-seeded kill loop with CH-down windows injected
