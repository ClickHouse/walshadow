-- 64 nested documents per kind. lz4 leaves 'a' at ~3.5 KB, two TOAST chunks
-- (production fetches ~3.5 KB per value), and 'b' at ~2 KB, one chunk
CREATE TABLE ceramic_pool (id int, kind text, doc jsonb, PRIMARY KEY (kind, id));
INSERT INTO ceramic_pool SELECT g, 'a', jsonb_build_object('version', g, 'kind', 'item',
  'items', (SELECT jsonb_agg(jsonb_build_object('id', i, 'name', 'item-' || md5(g || 'n' || i),
     'score', round(random()::numeric, 6),
     'tags', jsonb_build_array(substr(md5(g || 't' || i), 1, 8), substr(md5(g || 'u' || i), 1, 8)),
     'attrs', jsonb_build_object('color', (ARRAY['red', 'green', 'blue'])[1 + i % 3],
       'weight', i * 1.5, 'note', md5(g || 'a' || i))))
   FROM generate_series(1, 28) i),
  'summary', jsonb_build_object('count', 28, 'owner', md5(g::text), 'ok', true))
FROM generate_series(0, 63) g;
INSERT INTO ceramic_pool SELECT g, 'b', jsonb_build_object('result', g, 'status', 'ok',
  'outputs', (SELECT jsonb_agg(jsonb_build_object('key', 'out-' || i, 'value', md5(g || 'o' || i),
     'ms', i * 7, 'trace', substr(md5(g || 'r' || i), 1, 16)))
   FROM generate_series(1, 30) i))
FROM generate_series(0, 63) g;
