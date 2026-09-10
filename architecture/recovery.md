# Restart, retention, and timeline crossing

![Restart and cleanup](recovery.svg)

Transport receipt, local WAL durability, shadow replay, and ClickHouse
acknowledgement prove different things. Restart needs enough history to rebuild
every transaction whose destination effects are not yet durable

## Durable progress

Inserts can finish out of order. Acknowledgement collector advances only through
contiguous completed work. Transaction buffer further limits restart position
to earliest record needed by open or committed-but-unacknowledged transactions
Persisted manifest also clamps progress to durable filtered WAL

Persist manifest before publishing a new floor to cleanup or source feedback
This keeps every deletion bounded by a position restart can actually use
Process-local transaction spill is rebuilt from retained WAL; descriptor history,
backfill state, and deferred cleanup intent survive independently

Restart may resend rows already inserted near checkpoint. Source row keys and
LSN versions make normal replay converge. Losing required WAL or durable history
is an error, not permission to skip ahead

## Separate retention needs

Decoder history and TOAST cleanup follow persisted restart floor. Filtered WAL
needed by shadow also depends on its restartpoint, since a restarted PostgreSQL
may need records older than its latest replay position

Source feedback must preserve WAL until downstream work is safe. Highest received
or decoded position alone cannot authorize physical-slot recycling. Exact
position calculations belong in [manifest](../src/source/manifest.rs),
[source feedback](../src/source/source_feed.rs), and
[retention](../src/ops/retention.rs)

## Planned source crossing

Pause freezes consumed and received source frontiers while accepted destination
work drains. Before promotion, target must prove it owns required source history
and its physical slot protects resume position. After promotion, daemon verifies
system identity, ancestry, fork position, and shared segment prefix

A pipeline barrier separates ancestor and descendant work. Persist branch-aware
resume state before restart could confuse filenames or source identities
History remains available while retained WAL can refer to it

Missing proof stops crossing with a named reason. It cannot be replaced by an
operator assertion that a server looks like correct primary. Procedure lives in
[switchover guide](../docs/failover.md); unplanned cases remain in
[failover plan](../plans/failover.md)

Start implementation reading in [daemon](../src/bin/stream.rs),
[transaction buffer](../src/xact/xact_buffer.rs),
[acknowledgements](../src/emit/pipeline/ack.rs), and
[source transition](../src/source/transition.rs)
