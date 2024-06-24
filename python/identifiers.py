import time
import os
import random
from pprint import pprint
from datetime import timedelta

import psycopg2
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv('../.env')

# TODO: criar e truncar tabelas antes dos testes
# TODO: salvar os resultados como CSV
# TODO: imprimir resultados em formato de tabela


INSERT_TIER_1 = 1_000_000 // 1_000
INSERT_TIER_2 = 10_000_000 // 1_000
BATCH_SIZE_TIER_1 = 1_000 // 1_000
BATCH_SIZE_TIER_2 = 10_000 // 1_000
BATCH_SIZE_TIER_3 = 100_000 // 1_000
NUM_SIMULATIONS = 1_000_000 // 1_000
SKIP_ROWS = INSERT_TIER_2


def connect_db():
    return psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASS'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )


def test_insert_many_records(conn, table_name, num_records, batch_size):
    with conn.cursor() as cur:
        tqdm_desc = f'Inserting {num_records} records in batch of {batch_size} into {table_name}'
        tqdm_miniters = int(num_records / (100 * batch_size))
        start_time = time.time()
        for _ in tqdm(range(0, num_records, batch_size), desc=tqdm_desc, miniters=tqdm_miniters):
            query = f'INSERT INTO {table_name} (data) VALUES ' + ', '.join(['(%s)'] * batch_size)
            data = ['some_data'] * batch_size
            cur.execute(query, data)
        conn.commit()
        end_time = time.time()
        measurements[table_name][f'insert_{num_records}_batched_{batch_size}'] = (end_time - start_time) * 1000


def test_fetch_many_records(conn, table_name, num_records):
    print(f'Fetching {num_records} records from {table_name}')
    with conn.cursor() as cur:
        start_time = time.time()
        cur.execute(f'SELECT id FROM {table_name} LIMIT %s', (num_records,))
        records = cur.fetchall()
        end_time = time.time()
        measurements[table_name]['fetch_many_records'] = (end_time - start_time) * 1000
    return [record[0] for record in records]


def test_fetch_record_by_id(conn, table_name, ids, num_simulations):
    with conn.cursor() as cur:
        total_time = 0
        for _ in tqdm(range(num_simulations), desc=f'Fetching record by ID from {table_name}', miniters=(num_simulations/100)):
            id = random.choice(ids)
            start_time = time.time()
            cur.execute(f'SELECT * FROM {table_name} WHERE id = %s', (id,))
            cur.fetchone()
            end_time = time.time()
            total_time += end_time - start_time
        measurements[table_name]['fetch_record_by_id'] = (total_time / num_simulations) * 1000


def test_fetch_random_record(conn, table_name, skip_rows, num_simulations):
    with conn.cursor() as cur:
        total_time = 0
        for _ in tqdm(range(num_simulations), desc=f'Fetching random record from {table_name}', miniters=(num_simulations/100)):
            start_time = time.time()
            cur.execute(f'SELECT id FROM {table_name} OFFSET {skip_rows} LIMIT 1')
            cur.fetchone()
            end_time = time.time()
            total_time += end_time - start_time
        measurements[table_name]['fetch_random_record'] = (total_time / num_simulations) * 1000


def test_count_records(conn, table_name):
    print(f'Counting records in {table_name}')
    with conn.cursor() as cur:
        start_time = time.time()
        cur.execute(f'SELECT COUNT(*) FROM {table_name}')
        count = cur.fetchone()[0]
        end_time = time.time()
        measurements[table_name]['count_records'] = (end_time - start_time) * 1000
    return count


def test_indexes_size(conn, table_name):
    print(f'Calculating indexes size for {table_name}')
    with conn.cursor() as cur:
        cur.execute('SELECT pg_size_pretty(pg_indexes_size(%s))', (table_name,))
        size = cur.fetchone()[0]
        measurements[table_name]['indexes_size'] = size
    return size


def analyse_table(conn):
    with conn.cursor() as cur:
        cur.execute('ANALYZE')
        conn.commit()


def truncate_table(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(f'TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE')
        conn.commit()


def run_tests(conn, table_name):
    measurements[table_name] = {}

    for batch_size in (1, BATCH_SIZE_TIER_1, BATCH_SIZE_TIER_2):
        test_insert_many_records(conn, table_name, INSERT_TIER_1, batch_size)
        analyse_table(conn)
        truncate_table(conn, table_name)

    for batch_size in (BATCH_SIZE_TIER_1, BATCH_SIZE_TIER_2):
        test_insert_many_records(conn, table_name, INSERT_TIER_2, batch_size)
        analyse_table(conn)
        truncate_table(conn, table_name)

    # Keep the records for the next test
    test_insert_many_records(conn, table_name, INSERT_TIER_2, BATCH_SIZE_TIER_3)
    analyse_table(conn)

    records = test_fetch_many_records(conn, table_name, NUM_SIMULATIONS)
    test_fetch_record_by_id(conn, table_name, records, NUM_SIMULATIONS)
    test_fetch_random_record(conn, table_name, SKIP_ROWS, NUM_SIMULATIONS)

    test_count_records(conn, table_name)

    test_indexes_size(conn, table_name)

    print()

# Tests
# - 1M inserts, batch size = 10k
# - 1M inserts, batch size = 1k
# - 1M inserts
# - 10M inserts, batch size = 100k
# - 10M inserts, batch size = 10k
# - 10M inserts, batch size = 1k
# - Get 1M IDs
# - Get by ID from 20M records, simulation size = 1M, random ids = 1M
# - Skip 10M records and take 1, simulation size = 1M
# - Count, simulation size = 1M
# - Index size with 20M records
measurements = {}

def main():
    conn = connect_db()

    tables = [
        't_serial',
        't_uuid_v4',
        't_uuid_v4_custom',
        't_uuid_v7_custom',
    ]

    for table in tables:
        run_tests(conn, table)

    pprint(measurements)

    conn.close()


if __name__ == '__main__':
    start_time = time.time()
    main()
    end_time = time.time()
    total_time = timedelta(seconds=end_time - start_time)
    print(f'Total Time: {total_time}')
