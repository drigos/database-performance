\set random_offset random(0, int(:max))
SELECT id AS id FROM t_ulid_type_base32 OFFSET :random_offset LIMIT 1 \gset
SELECT * FROM t_ulid_type_base32 WHERE id = ':id'
