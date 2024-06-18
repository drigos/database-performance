\set random_offset random(0, int(:max))
SELECT id AS id FROM t_serial OFFSET :random_offset LIMIT 1 \gset
SELECT * FROM t_serial WHERE id = ':id'
