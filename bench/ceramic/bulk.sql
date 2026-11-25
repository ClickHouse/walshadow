\set first random(1, :seed_rows - :bulk_rows + 1)
UPDATE ceramic_items SET status = CASE status WHEN 'pending' THEN 'running' ELSE 'pending' END,
  attempt = attempt + 1, updated_at = now()
  WHERE id BETWEEN :first::bigint AND :first::bigint + :bulk_rows::int - 1;
