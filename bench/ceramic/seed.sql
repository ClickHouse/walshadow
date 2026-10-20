-- :rows hot-table rows, each with out-of-line payload_a (two chunks) and
-- payload_b (one chunk) the shadow holds, and :rows/10 parent runs
INSERT INTO ceramic_items
  (parent_id, group_id, node_id, list_id, seq_no, status, started_at,
   deleted_at, duration_ms, priority, shard, owner_id, cost, units_in, worker_name,
   trace_id, idempotency_key, payload_a, payload_b, payload_c, payload_d,
   source_ids)
SELECT g / 10 + 1, g % 50000, g % 200000, CASE WHEN g % 5 = 0 THEN g % 100000 END, g % 5000,
       'pending', now() - interval '1 hour', CASE WHEN g % 4 = 0 THEN now() END, g % 90000,
       g % 10, g % 32, g % 500, g * 7, g % 8000, 'worker-' || g % 32, md5(g::text),
       md5((g * 31)::text), d.doc, o.doc,
       CASE WHEN g % 3 = 0 THEN jsonb_build_object('units', g % 1000, 'detail', repeat(md5((g + 2)::text), 8)) END,
       jsonb_build_object('args', g % 41, 'text', repeat(md5((g + 3)::text), 5)),
       ARRAY[md5(g::text)]
FROM generate_series(1, :rows) g
JOIN ceramic_pool d ON d.kind = 'a' AND d.id = g % 64
JOIN ceramic_pool o ON o.kind = 'b' AND o.id = (g * 7) % 64;
INSERT INTO ceramic_runs (group_id, owner_id, status, item_count, duration_ms,
  finished_at, shard, input_a, state_a, headers_a, origin_a)
SELECT g % 50000, g % 500, 'running', 10, g % 600000, now(), g % 32,
       jsonb_build_object('args', repeat(md5(g::text), 12)), jsonb_build_object('s', repeat(md5((g + 1)::text), 5)),
       jsonb_build_object('h', repeat(md5((g + 2)::text), 5)), jsonb_build_object('o', repeat(md5((g + 3)::text), 4))
FROM generate_series(1, :rows / 10) g;
INSERT INTO ceramic_lists (owner_id, name, row_count)
SELECT g % 500, 'list-' || g, g FROM generate_series(1, 100000) g;
INSERT INTO ceramic_nodes (group_id, kind, config)
SELECT g % 50000, 'task', jsonb_build_object('n', g) FROM generate_series(1, 200000) g;
