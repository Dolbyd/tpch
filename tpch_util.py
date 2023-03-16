import os
import time
import psycopg2
from datetime import datetime


def open_connection():
    try:
        conn = psycopg2.connect(
            user="postgres",
            password="qaz1wsx2",
            host="localhost",
            database="sqream"
        )
        print("Database connected successfully")
    except():
        print("Database not connected")

    cursor = conn.cursor()
    return cursor, conn


def create_schema(cursor, conn):
    cursor.execute("DROP TABLE IF EXISTS nation, region, part, supplier, partsupp, customer, orders, lineitem")

    cursor.execute(open("tpch-schema.sql", "r").read())
    conn.commit()


def load_data(cursor, conn):
    folder_path = "tpch_data"

    for file_name in os.listdir(folder_path):
        table_name = file_name[:-4]
        file_path = os.path.join(folder_path, file_name)

        with open(file_path, 'r') as f:
            cursor.copy_from(f, table_name, sep='|')

    conn.commit()


def run_benchmark(cursor):
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


def run_benchmark_save_results(cursor, conn):
    cursor.execute('''CREATE TABLE IF NOT EXISTS tpch_results  
             (run_datetime TIMESTAMP, tpch_query_name TEXT, benchmark_result NUMERIC)''')

    table_name = "tpch_results"
    result_list = run_benchmark(cursor)
    for result in result_list:
        run_datetime = result[0]
        tpch_query_name = result[1]
        benchmark_result = result[2]
        cursor.execute(
            f"INSERT INTO {table_name} (run_datetime, tpch_query_name, benchmark_result) VALUES (%s, %s, %s)",
            (run_datetime, tpch_query_name, benchmark_result))

    conn.commit()


def fetch_results(cursor):
    cursor.execute("SELECT * FROM tpch_results")
    rows = cursor.fetchall()
    for row in rows:
        print(f"Run datetime: {row[0]}, TPCH query name: {row[1]}, Benchmark result: {row[2]}")


def close_connection(cursor, conn):
    cursor.close()
    conn.close()
