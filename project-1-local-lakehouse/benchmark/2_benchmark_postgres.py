import gzip
import psycopg2
import time
from pathlib import Path

DATA_DIR = Path("/data")
DATA_FILE = DATA_DIR / "2019-Oct.csv.gz"

DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "marketplace",
    "user": "root",
    "password": "root",
}

TABLE_NAME = "raw_events"

def connect():
    return psycopg2.connect(**DB_CONFIG)

def create_table(cur):
    with gzip.open(DATA_FILE, "rt") as f:
        header = f.readline().strip().split(",")
    columns = ", ".join(f'"{c}" TEXT' for c in header)
    cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS raw.{TABLE_NAME} (
            {columns}
        );
    """)

def table_has_data(cur):
    cur.execute(f"SELECT COUNT(*) FROM raw.{TABLE_NAME};")
    return cur.fetchone()[0] > 0

def load_data(cur):
    print("Loading data into Postgres...")
    with gzip.open(DATA_FILE, "rt") as f:
        cur.copy_expert(f"COPY raw.{TABLE_NAME} FROM STDIN WITH CSV HEADER", f)
    print("Load complete.")

def print_table_size(cur):
    cur.execute(f"""
        SELECT pg_size_pretty(
            pg_total_relation_size('raw.{TABLE_NAME}')
        );
    """)
    size = cur.fetchone()[0]
    print("Table size:", size)

def run_query(cur):
    print("Running benchmark query...")
    start = time.time()
    cur.execute(f"""
        SELECT event_type, COUNT(DISTINCT user_session)
        FROM raw.{TABLE_NAME}
        WHERE event_time::date = '2019-10-01'
        GROUP BY event_type;
    """)
    result = cur.fetchall()
    duration = time.time() - start

    print("Query results:")
    for row in result:
        print(row)
    print(f"Query time: {duration:.2f} s")

def main():
    conn = connect()
    try:
        with conn.cursor() as cur:
            create_table(cur)
            conn.commit()

            if table_has_data(cur):
                print("Data already loaded. Skipping load.")
            else:
                load_data(cur)
                conn.commit()

            print_table_size(cur)
            run_query(cur)
    finally:
        conn.close()

if __name__ == "__main__":
    main()