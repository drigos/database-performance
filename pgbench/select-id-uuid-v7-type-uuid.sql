\set random_offset random(0, int(:max))
SELECT id AS id FROM t_uuid_v4_type_uuid OFFSET :random_offset LIMIT 1 \gset
SELECT * FROM t_uuid_v4_type_uuid WHERE id = ':id'
