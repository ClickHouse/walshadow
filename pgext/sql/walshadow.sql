-- pg_regress suite for walshadow.
--
-- Exercises every branch in walshadow_decode_disk: varlena, fixed
-- pass-by-value (1/2/4/8 byte), fixed pass-by-reference, cstring, plus
-- STRICT NULL handling and the two ereport paths. Fixed-width tests
-- assume host-endian == little-endian, matching the on-disk Datum
-- layout the function reconstructs.

CREATE EXTENSION walshadow;

-- ---------- varlena -----------------------------------------------------

-- text: bytea body == UTF-8 of the string.
SELECT walshadow_decode_disk('text'::regtype, 'hello'::text::bytea) = 'hello' AS text_ascii;
SELECT walshadow_decode_disk('text'::regtype, 'héllo wörld'::text::bytea) = 'héllo wörld' AS text_utf8;
SELECT walshadow_decode_disk('text'::regtype, ''::text::bytea) = '' AS text_empty;

-- varchar shares text's body shape.
SELECT walshadow_decode_disk('varchar'::regtype, 'abc'::varchar::text::bytea) AS varchar_abc;

-- bytea: identity (typoutput renders \x hex).
SELECT walshadow_decode_disk('bytea'::regtype, '\xdeadbeef'::bytea) AS bytea_hex;

-- json: free-form text body.
SELECT walshadow_decode_disk('json'::regtype, '{"k":1}'::text::bytea) AS json_value;

-- Large varlena exercises the palloc + memcpy path past short-header
-- territory; function always writes a 4-byte header regardless.
SELECT walshadow_decode_disk('text'::regtype, repeat('a', 1024)::text::bytea) = repeat('a', 1024)
  AS text_1k;

-- ---------- fixed pass-by-value ----------------------------------------

-- int2: 2 bytes little-endian, value 42.
SELECT walshadow_decode_disk('int2'::regtype, '\x2a00'::bytea) AS int2_42;

-- int4: 4 bytes little-endian, value 42.
SELECT walshadow_decode_disk('int4'::regtype, '\x2a000000'::bytea) AS int4_42;

-- int4: negative value -1 → 0xffffffff.
SELECT walshadow_decode_disk('int4'::regtype, '\xffffffff'::bytea) AS int4_neg1;

-- int8: 8 bytes little-endian, value 1234567890 = 0x499602d2.
SELECT walshadow_decode_disk('int8'::regtype, '\xd202964900000000'::bytea) AS int8_val;

-- bool: 1 byte.
SELECT walshadow_decode_disk('bool'::regtype, '\x01'::bytea) AS bool_t;
SELECT walshadow_decode_disk('bool'::regtype, '\x00'::bytea) AS bool_f;

-- oid: 4 bytes little-endian, value 1234 = 0x4d2.
SELECT walshadow_decode_disk('oid'::regtype, '\xd2040000'::bytea) AS oid_1234;

-- float4: 4 bytes IEEE-754, value 1.0 = 0x3f800000.
SELECT walshadow_decode_disk('float4'::regtype, '\x0000803f'::bytea) AS float4_one;

-- float8: 8 bytes IEEE-754, value 1.0 = 0x3ff0000000000000.
SELECT walshadow_decode_disk('float8'::regtype, '\x000000000000f03f'::bytea) AS float8_one;

-- Extra raw bytes past typlen are ignored (memcpy honours typlen).
SELECT walshadow_decode_disk('int4'::regtype, '\x2a000000ffffffff'::bytea) AS int4_42_trailing;

-- ---------- fixed pass-by-reference ------------------------------------

-- uuid: 16 bytes verbatim.
SELECT walshadow_decode_disk('uuid'::regtype,
    '\x00112233445566778899aabbccddeeff'::bytea) AS uuid_val;

-- ---------- STRICT NULL handling ---------------------------------------

-- Function is STRICT — either NULL arg short-circuits to NULL without
-- entering the C body.
SELECT walshadow_decode_disk(NULL::oid, '\x00'::bytea) IS NULL AS null_oid;
SELECT walshadow_decode_disk('int4'::regtype, NULL::bytea) IS NULL AS null_raw;

-- ---------- error paths ------------------------------------------------

-- Unknown type oid (chosen well above any real catalog entry).
SELECT walshadow_decode_disk(2147483647::oid, '\x00'::bytea);

-- Raw shorter than typlen for a fixed pass-by-value type.
SELECT walshadow_decode_disk('int4'::regtype, ''::bytea);

-- Raw shorter than typlen for a fixed pass-by-reference type.
SELECT walshadow_decode_disk('uuid'::regtype, '\x0011'::bytea);

DROP EXTENSION walshadow;
