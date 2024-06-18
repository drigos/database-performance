import time
import os
import random
from datetime import timedelta

import psycopg2
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv('../.env')


NUM_RECORDS = 10_000
BATCH_SIZE = 1
NUM_SIMULATIONS = int(NUM_RECORDS / 100)


def connect_db():
    return psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASS'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )


def print_banner():
    print('-' * 40)
    print(f'NUM_RECORDS (R): {NUM_RECORDS}')
    print(f'BATCH_SIZE (B): {BATCH_SIZE}')
    print(f'NUM_INSERTS (I): {NUM_RECORDS // BATCH_SIZE}')
    print(f'NUM_SIMULATIONS (S): {NUM_SIMULATIONS}')
    print('-' * 40)
    print()


def insert_many_records(conn, table_name, num_records=NUM_RECORDS, batch_size=BATCH_SIZE):
    with conn.cursor() as cur:
        tqdm_desc = f'Inserting data into {table_name}'
        tqdm_miniters = int(num_records / (100 * batch_size))
        start_time = time.time()
        for _ in tqdm(range(0, num_records, batch_size), desc=tqdm_desc, miniters=tqdm_miniters):
            query = f'INSERT INTO {table_name} (data) VALUES ' + ', '.join(['(%s)'] * batch_size)
            data = ['some_data'] * batch_size
            cur.execute(query, data)
        conn.commit()
        end_time = time.time()
    insert_time_records[table_name] = end_time - start_time


def insert_one_record(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(f'INSERT INTO {table_name} (data) VALUES (%s) RETURNING id', ('some_data',))
        id = cur.fetchone()[0]
        conn.commit()
    return id


def fetch_many_records(conn, table_name, num_records=NUM_SIMULATIONS):
    with conn.cursor() as cur:
        start_time = time.time()
        cur.execute(f'SELECT id FROM {table_name} LIMIT {num_records}')
        records = cur.fetchall()
        end_time = time.time()
    batch_query_time_records[table_name] = (end_time - start_time) * 1000
    return [record[0] for record in records]


def fetch_one_record(conn, table_name, id):
    with conn.cursor() as cur:
        start_time = time.time()
        cur.execute(f"SELECT * FROM {table_name} WHERE id = %s", (id,))
        cur.fetchone()
        end_time = time.time()
    query_time_records[table_name] = (end_time - start_time) * 1000


def monte_carlo_simulation(conn, table_name, ids, num_simulations=NUM_SIMULATIONS):
    with conn.cursor() as cur:
        total_time = 0
        for _ in tqdm(range(num_simulations), desc=f'Querying data from {table_name}', miniters=(num_simulations/100)):
            id = random.choice(ids)
            start_time = time.time()
            cur.execute(f"SELECT * FROM {table_name} WHERE id = %s", (id,))
            cur.fetchone()
            end_time = time.time()
            total_time += end_time - start_time
        avg_query_time_records[table_name] = (total_time / num_simulations) * 1000


def rank_performance(records):
    return sorted(records.items(), key=lambda x: x[1])


def print_performance_ranking(records):
    ranking = rank_performance(records)
    print()
    print('Performance Ranking:')
    for i, (table, time) in enumerate(ranking):
        print(f'{i+1}. {format(round(time, 6), ".6f")}: {table}')
    print()


def print_insert_time(records):
    for table, time in records.items():
        print(f'Insert Time: {format(round(time, 6), ".6f")} seconds ({table})')


def print_query_time(records):
    for table, time in records.items():
        print(f'Query Time: {format(round(time, 6), ".6f")} milliseconds ({table})')


def print_batch_query_time(records):
    for table, time in records.items():
        print(f'Batch Query Time: {format(round(time, 6), ".6f")} milliseconds ({table})')


def print_avg_query_time(records):
    for table, time in records.items():
        print(f'Avg Query Time: {format(round(time, 6), ".6f")} milliseconds ({table})')


tables = [
    't_serial',
    't_uuid_v4_type_uuid',
    't_uuid_v7_type_uuid',
    't_ulid_type_uuid',
    't_uuid_v4_type_char',
    't_uuid_v7_type_char',
    't_ulid_type_char',
    't_ulid_type_base32'
]

insert_time_records = {}
query_time_records = {}
batch_query_time_records = {}
avg_query_time_records = {}

def main():
    print_banner()

    conn = connect_db()

    for table in tables:
        insert_many_records(conn, table)

    print()
    print_insert_time(insert_time_records)
    print_performance_ranking(insert_time_records)

    with conn.cursor() as cur:
        print('Analyzing tables...\n')
        cur.execute('ANALYZE')
        conn.commit()

    for table in tables:
        id = insert_one_record(conn, table)
        fetch_one_record(conn, table, id)

    print_query_time(query_time_records)
    print_performance_ranking(query_time_records)

    for table in tables:
        records = fetch_many_records(conn, table)
        monte_carlo_simulation(conn, table, records)

    print()
    print_batch_query_time(batch_query_time_records)
    print_performance_ranking(batch_query_time_records)
    print_avg_query_time(avg_query_time_records)
    print_performance_ranking(avg_query_time_records)

    conn.close()


if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    total_time = timedelta(seconds=end_time - start_time)
    print(f'Total Time: {total_time}')
