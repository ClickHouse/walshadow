\set run_id random(1, 2000000)
\set workflow_id random(1, 50000)
\set node_id random(1, 200000)
\set step_index random(0, 40)
\set attempt random(0, 2)
\set dur random(5, 90000)
\set prio random(0, 9)
\set shard random(0, 31)
\set tenant random(1, 500)
\set cost random(100, 9000000)
\set tin random(0, 8000)
\set tout random(0, 8000)
INSERT INTO bench_workflow_run_steps
  (run_id, workflow_id, node_id, step_index, attempt, status, kind, started_at,
   finished_at, duration_ms, retry_count, priority, shard, tenant_id, cost_micros,
   tokens_in, tokens_out, bytes_in, bytes_out, exit_code, is_terminal, region,
   runner, trace_id, span_id, idempotency_key, input, output, error, metadata)
SELECT
   :run_id::bigint + g, :workflow_id::bigint, :node_id::bigint + g, :step_index::int + g,
   :attempt::int,
   CASE WHEN :attempt::int = 0 THEN 'succeeded' ELSE 'retried' END, 'task',
   now() - (:dur::text || ' ms')::interval, now(), :dur::int, :attempt::int,
   :prio::int, :shard::int, :tenant::bigint, :cost::bigint,
   :tin::int, :tout::int, :tin::int * 4, :tout::int * 4, 0, true, 'region-a',
   'runner-' || :shard::text, md5((:run_id::bigint + g)::text), md5(:node_id::text),
   md5((:run_id::bigint + g)::text || :step_index::text),
   jsonb_build_object('args', :step_index::int),
   jsonb_build_object('result', :dur::int),
   NULL::text,
   jsonb_build_object('attempt', :attempt::int, 'shard', :shard::int)
FROM generate_series(0, 5) g;
