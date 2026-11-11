# Observability model

Problem: metric families grew one incident at a time. Main carries about 190
families and 24 `_seconds` wall clocks, nothing documents what belongs in a
metric versus a span versus a log, and the production surface (dashboards in
`data-plane-application/o11y/dashboarding/pkg/dashboards/walshadow`, alerts in
`helm-charts/lighthouse-base/templates/rules/datagres.yaml`) reads about a third
of them. The metrics PR stack (#248, #250, #253, #252, #255, #256, #314, #315)
established the rules below; the migration of the pre-existing families is the
open work.

## Tiers

1. **SLIs**, few and alertable: commit-time lag, byte lag per position,
   delivered rows, errors by component and kind, source mode.
2. **Episodes and saturation**: the stage layer (`walshadow_stage_*`) is the
   always-on coarse span layer; `walshadow_worker_seconds_total{worker,state}`
   is the one saturation family for every serial worker. New phase timers join
   these rather than minting families.
3. **External round trips**: `walshadow_request_duration_seconds{target,op}`
   histograms, bridge today, ClickHouse insert and archive fetch to come.
4. **Inside a worker**: sampled spans and profiles, no sub-phase counters
   subtracted from busy.

Incident-only questions go to spans, profiles or logs. A suspected bug is fixed
or logged, not counted.

## Admission rule

A new family must name the panel or alert it feeds in data-plane-application,
or fit a tier above. Before adding one, check whether the number is a member of
an existing class; if it is not and a class is the right shape, found the class
through `label_class!` (`src/ops/metrics.rs`) so the label set is declared once
and shared by the snapshot field and the exposition.

Classes are keyed by the question a number answers, not by its unit. A class
whose `sum()` over all members is a number nobody wants (every byte flow in the
daemon, every event of any kind) is too wide; a class with one member and no
expected siblings is too narrow.

## Classes in the stack

| Family | Labels | Members today | Expected members |
|---|---|---|---|
| `position_lsn` | `at` | source_head, received, filter, decoder_commit, ack, shadow_replay, floor, pause_consumed, pause_received, promotion_target_replay, promotion_target_receive, timeline_switch | complete |
| `commit_timestamp_seconds` | `at` | source, ack | shadow_replay once the pump keeps an LSN→commit-time ring |
| `source_mode` | `mode` | the seven one-hot states, Initial load included | complete |
| `stage_active`, `stage_seconds_total`, `stage_completed_total`, `stage_interrupted_total` | `stage`, `title`, `order` | ten stages | copy and publish phases as the table backfill grows |
| `stage_count` | `stage`, `title`, `order`, `measure` | work_total, work_done, rows | complete |
| `worker_seconds_total` | `worker`, `state` | pump, queue, batcher, inserter, backup_tap, backup_walk × busy, idle, blocked_in, blocked_out | reorder if it ever leaves the queue worker |
| `errors_total` | `component`, `kind` | source: wal_removed, disconnect, walsender_exit, other | ClickHouse insert retries and reconnects, DDL retries, bridge errors, oracle errors, timeline switch failures, endpoint swap failures |
| `request_duration_seconds` | `database`, `target`, `op` | bridge × op | clickhouse insert, archive fetch |
| `wal_bytes_total` | `flow` | walsender_backfill | backup_tapped, backfill_backup, backfill_copy, archive_fetched, timeline_prefix_verified |
| `buffer_bytes` | `buffer` | walsender_send_queue | xact_in_memory, spill, drain_resident, toast_spool, resident_payload, desc_log_tail |
| `bootstrap_gate_tuples_total` | `verdict` | emitted, gated, deferred, pending | complete |
| `shadow_stream_sockets_total` | `event` | accepted, closed_slow_client, closed_by_client, closed_error | complete |
| `table_rows_inserted_total` | `database`, `table` | per table | complete |

## Migration map for pre-existing families

Target column: a class above, `keep` (standalone, user tier), `debug` (behind a
debug exposition flag, default off), or `derive` (dropped, the value is a PromQL
expression over a class). Dashboards and alerts move in the same change as the
family they read.

| Family | Target |
|---|---|
| `shadow_apply_lag_bytes` | derive: `position_lsn` received minus shadow_replay |
| `shadow_apply_lag_seconds` | derive from `commit_timestamp_seconds{at="shadow_replay"}` once it exists; the bytes÷rate estimate explodes when the pump's intake stops, and `WalshadowApplyLagSeconds` carries a `< +Inf` workaround for it |
| `emitter_rows_total` | derive: sum of `table_rows_inserted_total` |
| `archive_restore_active` | derive: `source_mode{mode="Catching up from archive"}` |
| `bootstrap_bytes_tapped`, `backfill_backup_bytes_total`, `backfill_copy_bytes_total`, `timeline_prefix_bytes_verified_total` | `wal_bytes_total{flow}` |
| `archive_wal_segments_total` | `wal_bytes_total{flow="archive_fetched"}` in bytes |
| `xact_bytes_in_memory`, `spill_bytes_active`, `drain_resident_bytes`, `drain_chunk_resident_bytes`, `drain_row_resident_bytes`, `toast_xact_spool_bytes`, `resident_payload_bytes`, `resident_payload_peak_bytes`, `raw_pending_bytes`, `desc_log_tail_bytes` | `buffer_bytes{buffer}` |
| `bootstrap_deferred_bytes`, `bootstrap_deferred_spool_bytes`, `bootstrap_deferred_replay_bytes`, `bootstrap_deferred_replayed_bytes` | derive: `stage_count` for bootstrap_deferred already carries the replay ratio; the spool byte gauges also wrap below zero (u64 underflow in the deferred footprint), fix or drop |
| `bootstrap_parts_total`, `bootstrap_parts_done`, `bootstrap_tuples_emitted`, `bootstrap_pages_walked`, `bootstrap_files_walked`, `bootstrap_files_skipped_unmapped`, `bootstrap_replay_covered_rows_total` | derive from `stage_count`; pages and files walked to debug |
| `bootstrap_gate_chunks_gated_total`, `bootstrap_gate_multixact_emitted_total` | debug |
| `emitter_retries_attempted_total`, `emitter_reconnects_total`, `bridge_errors_by_op`, `bridge_reconnects_total`, `oracle_errors_total`, `oracle_conversion_errors_total`, `timeline_switch_failures_by_reason`, `source_endpoint_swap_failures_total`, `catalog_boundary_hold_failures_total`, `toast_fetch_miss_total` | `errors_total{component,kind}` |
| `inserter_ch_seconds_total`, `archive_fetch_seconds_total` | `request_duration_seconds{target}` histograms; the counters go once the histograms exist |
| `inserter_encode_seconds_total`, `archive_wait_seconds_total`, `archive_replay_seconds_total`, `oracle_resolve_seconds_total`, `toast_chunk_put_seconds`, `toast_value_fetch_seconds`, `toast_value_wait_seconds`, `decoder_buffer_lock_wait_seconds_total`, `catalog_boundary_hold_seconds_total`, `pending_hold_seconds_total`, `desc_capture_seconds_total`, `timeline_transition_seconds_total`, `bridge_request_seconds_by_op`, `bridge_lock_wait_seconds_by_op`, `bridge_service_seconds_by_op` | debug; `worker_seconds_total` and the histograms carry the operator view |
| `xacts_committed_total`, `xacts_aborted_total`, `emitter_xacts_total`, `decoder_decoded_total`, `decoder_records_by_op`, `raw_decode_records_by_kind_op`, `raw_decode_rows_by_op`, `queue_records_out_total`, `queue_jobs_out_total`, `decode_jobs_in_total`, `decode_rows_out_total`, `insertbatch_rows_in_total`, `insertbatch_batches_out_total`, `inserter_batches_in_total`, `emitter_blocks_total`, `xact_plan_rows`, `xact_plan_bytes_by_storage`, `xact_plan_failures_by_reason` | debug: pipeline internals; `queue_records_out_total` stays in the user tier as the WAL record throughput until `wal_bytes_total{flow="received"}` exists |
| `pump_queue_depth` | keep |
| `shadow_stream_active_connections`, `bridge_up`, `crossing_wedged`, `config_backfills_pending_by_mode`, `uptime_seconds_total`, `process_cpu_seconds_total`, `process_resident_memory_bytes`, `process_threads` | keep |
| `source_endpoint_swaps_total`, `source_endpoint_swap_pending`, `timeline_switches_total`, `source_timeline`, `floor_timeline`, `shadow_served_timeline`, `shadow_replay_timeline`, `source_info`, `promotion_*`, `pause_refrozen` | keep as failover state; candidates for one `timeline{role}` gauge class |
| `catalog_boundary_holds_total`, `pending_*` (14), `desc_*` (21), `toast_*` chunk store and stash (22), `decoder_*` lock and skip counters, `memory_budget_*`, `spill_*`, `drain_*`, `raw_*`, `stash_*`, `oracle_*` cells and columns, `bridge_*` byte counters, `config_replicate_opt_*`, `route_snapshots_total`, `stage_epoch_seconds`, `bootstrap_attempt` | debug |

End state: about 25 families in the default exposition, 15 of them classes,
with the debug tier available on a flag.

## Alert work the model needs

- `WalshadowNotReplicating` signal 1 reads `position_lsn{at="received"} == bool
  0`; since #262 the member is absent at 0, so the signal cannot fire. Replace
  it with `source_mode{mode="Reconnecting to source"} == 1` held for the window.
- `WalshadowNotReplicating` signal 3 (`shadow_stream_active_connections == 0`)
  fires through every archive catch-up after a bootstrap; suppress on
  `source_mode{mode="Catching up from archive"}` as well as on the bootstrap
  and copy stages, or export one health gauge with a reason label.
- `WalshadowClickHouseLag` on `commit_timestamp_seconds{at="source"} -
  ignoring(at) commit_timestamp_seconds{at="ack"}` is blind during an archive
  catch-up, when both members are hours old and a minute apart. The SLI needs a
  wall-clock side: ack age with an idle-source caveat, or the source head's
  commit time fetched out of band.
- `WalshadowApplyLagSeconds` keeps its `< +Inf` workaround until the shadow
  replay commit-time member exists.

## Open

- Debug exposition flag for the debug tier.
- `docker/grafana` dashboard: trim to a local-dev view or delete; the
  production dashboards are the data-plane-application Go source.
- #250 and #252 still need PR bodies.
