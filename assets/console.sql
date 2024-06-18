SELECT 1;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

SELECT gen_random_uuid();

-- Função para ULID (formatado como UUID)
CREATE OR REPLACE FUNCTION custom_gen_ulid_type_uuid() RETURNS uuid
    AS $$
        SELECT (lpad(to_hex(floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint), 12, '0') || encode(gen_random_bytes(10), 'hex'))::uuid;
    $$ LANGUAGE SQL;

SELECT custom_gen_ulid_type_uuid();

-- Função para ULID (formatado como Crockford's Base 32)
CREATE OR REPLACE FUNCTION custom_gen_ulid_type_base32() RETURNS VARCHAR(26)
AS $$
DECLARE
    timestamp_ms BIGINT;
    random_bytes BYTEA;
    ulid_base32 TEXT;
    base32_alphabet TEXT := '0123456789ABCDEFGHJKMNPQRSTVWXYZ'; -- Adjusted alphabet
BEGIN
    timestamp_ms := floor(extract(epoch FROM clock_timestamp()) * 1000);

    ulid_base32 := '';
    WHILE timestamp_ms > 0 LOOP
        ulid_base32 := substr(base32_alphabet, ((timestamp_ms % 32)::INTEGER + 1)::INTEGER, 1) || ulid_base32;
        timestamp_ms := timestamp_ms / 32;
    END LOOP;

    ulid_base32 := lpad(ulid_base32, 10, '0');

    random_bytes := gen_random_bytes(16);
    FOR i IN 0..15 LOOP
        ulid_base32 := ulid_base32 || substr(base32_alphabet, (get_byte(random_bytes, i) % 32 + 1)::INTEGER, 1);
    END LOOP;

    RETURN ulid_base32;
END;
$$ LANGUAGE plpgsql;

SELECT custom_gen_ulid_type_base32();

CREATE OR REPLACE FUNCTION custom_gen_uuidv7() RETURNS uuid
AS $$
DECLARE
    uuidv7 TEXT;
BEGIN
    uuidv7 := (lpad(to_hex(floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint), 12, '0') || encode(gen_random_bytes(10), 'hex'));

    -- Ajustar os bits de versão (bytes 7)
    uuidv7 := substring(uuidv7, 1, 12) ||
              to_hex((get_byte(decode(substring(uuidv7, 13, 2), 'hex'), 0) & 0x0F) | 0x70) ||
              substring(uuidv7, 15);

    -- Ajustar os bits de variante (bytes 9)
    uuidv7 := substring(uuidv7, 1, 16) ||
              to_hex((get_byte(decode(substring(uuidv7, 17, 2), 'hex'), 0) & 0x3F) | 0x80) ||
              substring(uuidv7, 19);

    -- Adicionar hífens na posição correta
    uuidv7 := substring(uuidv7, 1, 8) || '-' ||
              substring(uuidv7, 9, 4) || '-' ||
              substring(uuidv7, 13, 4) || '-' ||
              substring(uuidv7, 17, 4) || '-' ||
              substring(uuidv7, 21);

    RETURN uuidv7::uuid;
END;
$$ LANGUAGE plpgsql;

SELECT custom_gen_uuidv7();

CREATE TABLE IF NOT EXISTS t_serial (
    id BIGSERIAL PRIMARY KEY,
    data TEXT
);

CREATE TABLE IF NOT EXISTS t_uuid_v4_type_uuid (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    data TEXT
);

CREATE TABLE IF NOT EXISTS t_uuid_v7_type_uuid (
    id UUID PRIMARY KEY DEFAULT custom_gen_uuidv7(),
    data TEXT
);

CREATE TABLE IF NOT EXISTS t_ulid_type_uuid (
    id UUID PRIMARY KEY DEFAULT custom_gen_ulid_type_uuid(),
    data TEXT
);

CREATE TABLE IF NOT EXISTS t_uuid_v4_type_char (
    id VARCHAR(36) PRIMARY KEY DEFAULT gen_random_uuid(),
    data TEXT
);

CREATE TABLE IF NOT EXISTS t_uuid_v7_type_char (
    id VARCHAR(36) PRIMARY KEY DEFAULT custom_gen_uuidv7(),
    data TEXT
);

CREATE TABLE IF NOT EXISTS t_ulid_type_char (
    id VARCHAR(36) PRIMARY KEY DEFAULT custom_gen_ulid_type_uuid(),
    data TEXT
);

CREATE TABLE IF NOT EXISTS t_ulid_type_base32 (
    id VARCHAR(26) PRIMARY KEY DEFAULT custom_gen_ulid_type_base32(),
    data TEXT
);

CREATE SEQUENCE test_sequence START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE test_sequence RESTART WITH 1;

EXPLAIN ANALYZE SELECT nextval('test_sequence') FROM generate_series(1, 10000000);
EXPLAIN ANALYSE SELECT gen_random_uuid() FROM generate_series(1, 10000000);
EXPLAIN ANALYSE SELECT custom_gen_uuidv7() FROM generate_series(1, 10000000);
EXPLAIN ANALYSE SELECT custom_gen_ulid_type_uuid() FROM generate_series(1, 10000000);
EXPLAIN ANALYSE SELECT custom_gen_ulid_type_base32() FROM generate_series(1, 10000000);

EXPLAIN ANALYSE INSERT INTO t_serial(id, data)
    SELECT nextval('test_sequence'), 'some_data' FROM generate_series(1, 10000000);
EXPLAIN ANALYSE INSERT INTO t_uuid_v4_type_uuid(id, data)
    SELECT gen_random_uuid(), 'some_data' FROM generate_series(1, 10000000);
EXPLAIN ANALYSE INSERT INTO t_uuid_v7_type_uuid(id, data)
    SELECT custom_gen_uuidv7(), 'some_data' FROM generate_series(1, 10000000);
EXPLAIN ANALYSE INSERT INTO t_ulid_type_uuid(id, data)
    SELECT custom_gen_ulid_type_uuid(), 'some_data' FROM generate_series(1, 10000000);
EXPLAIN ANALYSE INSERT INTO t_uuid_v4_type_char(id, data)
    SELECT gen_random_uuid(), 'some_data' FROM generate_series(1, 10000000);
EXPLAIN ANALYSE INSERT INTO t_uuid_v7_type_char(id, data)
    SELECT custom_gen_uuidv7(), 'some_data' FROM generate_series(1, 10000000);
EXPLAIN ANALYSE INSERT INTO t_ulid_type_char(id, data)
    SELECT custom_gen_ulid_type_uuid(), 'some_data' FROM generate_series(1, 10000000);
EXPLAIN ANALYSE INSERT INTO t_ulid_type_base32(id, data)
    SELECT custom_gen_ulid_type_base32(), 'some_data' FROM generate_series(1, 10000000);

EXPLAIN ANALYSE SELECT id FROM t_serial LIMIT 1 OFFSET 5000000;
EXPLAIN ANALYSE SELECT id FROM t_uuid_v4_type_uuid LIMIT 1 OFFSET 5000000;
EXPLAIN ANALYSE SELECT id FROM t_uuid_v7_type_uuid LIMIT 1 OFFSET 5000000;
EXPLAIN ANALYSE SELECT id FROM t_ulid_type_uuid LIMIT 1 OFFSET 5000000;
EXPLAIN ANALYSE SELECT id FROM t_uuid_v4_type_char LIMIT 1 OFFSET 5000000;
EXPLAIN ANALYSE SELECT id FROM t_uuid_v7_type_char LIMIT 1 OFFSET 5000000;
EXPLAIN ANALYSE SELECT id FROM t_ulid_type_char LIMIT 1 OFFSET 5000000;
EXPLAIN ANALYSE SELECT id FROM t_ulid_type_base32 LIMIT 1 OFFSET 5000000;

EXPLAIN ANALYSE SELECT id FROM t_serial WHERE id = '16997761';
EXPLAIN ANALYSE SELECT id FROM t_uuid_v4_type_uuid WHERE id = '3272dbcb-9a6b-463f-8a85-05988ce0c645';
EXPLAIN ANALYSE SELECT id FROM t_uuid_v7_type_uuid WHERE id = '018fd0cb-70fd-7bca-8516-cd3b0a725239';
EXPLAIN ANALYSE SELECT id FROM t_ulid_type_uuid WHERE id = '018fd0cc-024f-a0d4-693b-47d076d34b77';
EXPLAIN ANALYSE SELECT id FROM t_uuid_v4_type_char WHERE id = 'fd62aad7-8bc3-4743-a713-bc2aaf525e2e';
EXPLAIN ANALYSE SELECT id FROM t_uuid_v7_type_char WHERE id = '018fd0ce-0b36-7ff4-abe9-50fe4fd2eae0';
EXPLAIN ANALYSE SELECT id FROM t_ulid_type_char WHERE id = '018fd0cf-650d-4b00-983d-510233781a9d';
EXPLAIN ANALYSE SELECT id FROM t_ulid_type_base32 WHERE id = '01HZ8D0FHW97BD35TJA0HJ7T01';

TRUNCATE TABLE t_serial;
TRUNCATE TABLE t_uuid_v4_type_uuid;
TRUNCATE TABLE t_uuid_v7_type_uuid;
TRUNCATE TABLE t_ulid_type_uuid;
TRUNCATE TABLE t_uuid_v4_type_char;
TRUNCATE TABLE t_uuid_v7_type_char;
TRUNCATE TABLE t_ulid_type_char;
TRUNCATE TABLE t_ulid_type_base32;

DROP TABLE t_serial;
DROP TABLE t_uuid_v4_type_uuid;
DROP TABLE t_uuid_v7_type_uuid;
DROP TABLE t_ulid_type_uuid;
DROP TABLE t_uuid_v4_type_char;
DROP TABLE t_uuid_v7_type_char;
DROP TABLE t_ulid_type_char;
DROP TABLE t_ulid_type_base32;

SELECT count(*) FROM t_serial;
SELECT count(*) FROM t_uuid_v4_type_uuid;
SELECT count(*) FROM t_uuid_v7_type_uuid;
SELECT count(*) FROM t_ulid_type_uuid;
SELECT count(*) FROM t_uuid_v4_type_char;
SELECT count(*) FROM t_uuid_v7_type_char;
SELECT count(*) FROM t_ulid_type_char;
SELECT count(*) FROM t_ulid_type_base32;

SELECT
    schemaname,
    relname,
    n_live_tup,
    n_dead_tup,
    last_analyze,
    last_autoanalyze
FROM
    pg_stat_all_tables
WHERE
    relname LIKE 't_%';
