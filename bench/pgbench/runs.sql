\set workflow_id random(1, 50000)
\set tenant random(1, 500)
\set steps random(1, 60)
\set dur random(100, 600000)
\set prio random(0, 9)
\set shard random(0, 31)
\set p1 random(0, 63)
INSERT INTO bench_workflow_runs
  (workflow_id, tenant_id, status, trigger, step_count, failed_steps, duration_ms,
   finished_at, cost_micros, priority, shard, inputs, outputs, error)
VALUES
  (:workflow_id::bigint, :tenant::bigint, 'succeeded', 'api', :steps::int, 0,
   :dur::int, now(), :dur::bigint * 7, :prio::int, :shard::int,
   jsonb_build_object('payload', (SELECT body FROM bench_payload_pool WHERE id = :p1::int)),
   jsonb_build_object('steps', :steps::int), NULL::text);
