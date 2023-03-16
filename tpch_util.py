import os
import time
import psycopg2
from datetime import datetime
import argparse


def open_connection():
    db_conn = None
    try:
        db_conn = psycopg2.connect(
            user="postgres",
            password="qaz1wsx2",
            host="localhost",
            database="sqream"
        )
        print("Database connected successfully")
    except():
        print("Database not connected")

    db_cursor = db_conn.cursor()
    return db_cursor, db_conn


def create_schema():
    cursor.execute("DROP TABLE IF EXISTS nation, region, part, supplier, partsupp, customer, orders, lineitem")

    cursor.execute(open("tpch-schema.sql", "r").read())
    conn.commit()


def load_data():
    folder_path = "tpch_data"

    for file_name in os.listdir(folder_path):
        table_name = file_name[:-4]
        file_path = os.path.join(folder_path, file_name)

        with open(file_path, 'r') as f:
            cursor.copy_from(f, table_name, sep='|')

    conn.commit()


def run_benchmark():
    folder_path = "queries"
    result_list = []

    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        start_time = time.time()
        cursor.execute(open(file_path, "r").read())
        end_time = time.time()
        query_time = end_time - start_time
        current_time = datetime.now().replace()
        result = (current_time.strftime('%Y-%m-%d %H:%M:%S'), file_name, query_time)
        result_list.append(result)

    print(result_list)
    return result_list


def run_benchmark_save_results():
    cursor.execute('''CREATE TABLE IF NOT EXISTS tpch_results  
             (run_datetime TIMESTAMP, tpch_query_name TEXT, benchmark_result NUMERIC)''')

    table_name = "tpch_results"
    result_list = run_benchmark()
    for result in result_list:
        run_datetime = result[0]
        tpch_query_name = result[1]
        benchmark_result = result[2]
        cursor.execute(
            f"INSERT INTO {table_name} (run_datetime, tpch_query_name, benchmark_result) VALUES (%s, %s, %s)",
            (run_datetime, tpch_query_name, benchmark_result))

    conn.commit()


def fetch_results():
    cursor.execute("SELECT * FROM tpch_results")
    rows = cursor.fetchall()
    for row in rows:
        print(f"Run datetime: {row[0]}, TPCH query name: {row[1]}, Benchmark result: {row[2]}")


def close_connection():
    cursor.close()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Runs TPCH benchmark queries')
    parser.add_argument('--create_schema', action='store_true', help='Create database schema')
    parser.add_argument('--load_data', action='store_true', help='Load data into tables')
    parser.add_argument('--run-benchmark', action='store_true', help='run benchmark')
    parser.add_argument('--run-benchmark--save-results', action='store_true', help='run benchmark and save results')
    parser.add_argument('--fetch-results', action='store_true', help='fetch results and print')

    args = parser.parse_args()

    cursor, conn = open_connection()

    if args.create_schema:
        create_schema()
        print('database schema created successfully')

    if args.load_data:
        load_data()
        print('data loaded successfully')

    if args.run_benchmark:
        run_benchmark()
        print("run benchmark successfully")

    if args.run_benchmark__save_results:
        run_benchmark_save_results()
        print("run benchmark and save result successfully")

    if args.fetch_results:
        fetch_results()
        print("fetch and print results successfully")

    close_connection()
