from pathlib import Path
import duckdb
import time

DATA_DIR = Path("/tmp/data")
PARQUET_FILE = DATA_DIR / "2019-Oct.parquet"
ICEBERG_DIR = DATA_DIR / "iceberg_table"


def create_iceberg_if_missing(conn):
    """
    Create an Iceberg table from the Parquet file if it doesn't exist.
    DuckDB’s Iceberg extension supports read/write to local Iceberg tables.
    """
    conn.execute("INSTALL iceberg;")
    conn.execute("LOAD iceberg;")

    # Only create if the metadata folder doesn't exist
    if ICEBERG_DIR.exists() and any(ICEBERG_DIR.iterdir()):
        print(f"Iceberg table already exists at {ICEBERG_DIR}")
        return

    print("Creating Iceberg table from Parquet...")

    # Use standard SQL to create using Iceberg format
    conn.execute(f"""
        INSTALL iceberg;
LOAD iceberg;
                 
        CREATE TABLE IF NOT EXISTS events_iceberg
        USING ICEBERG
        LOCATION '{ICEBERG_DIR}'
        AS SELECT * FROM read_parquet('{PARQUET_FILE}');
    """)
    print("Iceberg table created.")


def main():
    conn = duckdb.connect()

    # Create Iceberg table once
    create_iceberg_if_missing(conn)

    # Estimate Iceberg folder size
    size_gb = sum(f.stat().st_size for f in ICEBERG_DIR.glob("**/*") if f.is_file()) / 1024**3
    print(f"Iceberg table size: {size_gb:.2f} GB")

    # Run the benchmark query
    start_query = time.time()
    result = conn.execute(f"""
        SELECT
            event_type,
            COUNT(DISTINCT user_session) AS distinct_sessions
        FROM events_iceberg
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