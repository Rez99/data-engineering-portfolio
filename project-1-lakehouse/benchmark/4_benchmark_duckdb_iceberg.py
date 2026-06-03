from pathlib import Path
import duckdb
import time

DATA_DIR = Path("/tmp/data")
CSV_FILE = DATA_DIR / "2019-Oct.csv.gz"
ICEBERG_DIR = DATA_DIR / "iceberg_table"


def create_iceberg_if_missing():
    if ICEBERG_DIR.exists():
        print(f"Iceberg table already exists at {ICEBERG_DIR}")
        return

    print("Creating Iceberg table from CSV...")

    conn = duckdb.connect()

    conn.execute(f"""
        CREATE SCHEMA IF NOT EXISTS tmp;
    """)

    conn.execute(f"""
        CREATE TABLE tmp.events
        USING ICEBERG
        LOCATION '{ICEBERG_DIR}'
        AS SELECT *
        FROM read_csv_auto('{CSV_FILE}');
    """)

    conn.close()
    print("Iceberg table created.")


def main():
    create_iceberg_if_missing()

    # Estimate storage size
    size_gb = sum(f.stat().st_size for f in ICEBERG_DIR.glob('**/*') if f.is_file()) / 1024**3
    print(f"Iceberg table size: {size_gb:.2f} GB")

    conn = duckdb.connect()

    start_query = time.time()
    result = conn.execute(f"""
        SELECT
            event_type,
            COUNT(DISTINCT user_session) AS distinct_sessions
        FROM read_iceberg('{ICEBERG_DIR}')
        WHERE CAST(event_time AS DATE) = DATE '2019-10-01'
        GROUP BY event_type
        ORDER BY event_type
    """).fetchdf()
    query_time = time.time() - start_query

    print("\nQuery results:")
    print(result.to_string(index=False))

    print(f"\nQuery time: {query_time:.2f} s")

    conn.close()


if __name__ == "__main__":
    main()