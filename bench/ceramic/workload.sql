-- One small transaction per run of this script, shaped after a production
-- waldump mix and status deltas. With probability upd_pm/1000 a status
-- transition: claim the parent run and the item with FOR UPDATE (two Heap
-- LOCK), then a non-HOT update that replaces the out-of-line payload_b
-- (chunk inserts plus deletes of the old chunks) and leaves the out-of-line
-- payload_a value unchanged, so walshadow fetches it from the shadow. With
-- probability ins_pm/1000 an upsert (HEAP_CONFIRM) of a new item carrying both
-- out-of-line values. Transition targets are the last recent_ids items with
-- probability recent_pm/1000, else random seeded items.
-- Insert and transition commit separately, as production commits once per
-- update. Variables from prepare.sh: seed_rows, upd_pm, ins_pm, recent_pm,
-- recent_ids
\set r1 random(0, 999)
\set r2 random(0, 999)
\set r3 random(0, 999)
\set parent_id random(1, :seed_rows / 10)
\set group_id random(1, 50000)
\set node_id random(1, 200000)
\set entry random(0, 5000)
\set list random(1, 100000)
\set shard random(0, 31)
\set owner random(1, 500)
\set p random(0, 63)
\set q random(0, 63)
\set dur random(5, 90000)
\set target random(1, :seed_rows)
\set lag random(1, :recent_ids)
\if :r1 < :ins_pm
INSERT INTO ceramic_items
  (parent_id, group_id, node_id, list_id, seq_no, shard, owner_id,
   units_in, worker_name, trace_id, idempotency_key, payload_a, payload_b, payload_d, source_ids)
VALUES (:parent_id::bigint, :group_id::bigint, :node_id::bigint,
   CASE WHEN :entry::int % 5 = 0 THEN :list::bigint END, :entry::int, :shard::int,
   :owner::bigint, :dur::int % 8000, 'worker-' || :shard::text, md5(:dur::text || :node_id::text),
   md5(:parent_id::text || :entry::text),
   (SELECT doc FROM ceramic_pool WHERE kind = 'a' AND id = :p::int),
   (SELECT doc FROM ceramic_pool WHERE kind = 'b' AND id = :q::int),
   jsonb_build_object('args', :entry::int, 'text', repeat(md5(:parent_id::text), 5)),
   ARRAY[md5(:node_id::text)])
ON CONFLICT (id) DO NOTHING;
\endif
\if :r2 < :upd_pm
BEGIN;
\if :r3 < :recent_pm
SELECT last_value - :lag AS target FROM ceramic_items_id_seq \gset
\endif
SELECT 1 FROM ceramic_runs WHERE id = :parent_id::bigint FOR UPDATE;
SELECT 1 FROM ceramic_items WHERE id = :target::bigint FOR UPDATE;
UPDATE ceramic_items SET status = CASE status WHEN 'pending' THEN 'running' ELSE 'succeeded' END,
  attempt = attempt + 1, completed_at = now(), duration_ms = :dur::int, updated_at = now(),
  payload_b = (SELECT doc FROM ceramic_pool WHERE kind = 'b' AND id = :q::int)
  WHERE id = :target::bigint;
COMMIT;
\endif
