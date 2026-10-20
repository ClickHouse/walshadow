\set list_id random(1, 100000)
\set chunk random(0, 999)
\set rows random(1, 5000)
\set p1 random(0, 63)
INSERT INTO bench_data_list_chunks (list_id, chunk_index, row_count, payload, checksum)
VALUES (:list_id::bigint, :chunk::int, :rows::int,
        jsonb_build_object('rows', (SELECT body FROM bench_payload_pool WHERE id = :p1::int)),
        md5(:list_id::text));
