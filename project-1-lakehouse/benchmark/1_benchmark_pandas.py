from pathlib import Path
import gzip
import pandas as pd
import time

DATA_DIR = Path("/tmp/data")
DATA_FILE = DATA_DIR / "2019-Oct.csv.gz"

def main():
    # Show file size
    size_gb = DATA_FILE.stat().st_size / 1024**3
    print(f"CSV.gz size: {size_gb:.2f} GB")

    # Load CSV into Pandas
    print("Loading CSV into Pandas...")
    start_load = time.time()
    df = pd.read_csv(DATA_FILE, compression='gzip')
    load_time = time.time() - start_load
    print(f"Load complete in {load_time:.2f} s")

    # Filter for Oct 1
    df['event_time'] = pd.to_datetime(df['event_time'])
    oct1 = df[df['event_time'].dt.date == pd.Timestamp('2019-10-01').date()]

    # Count distinct session_id per event_type
    start_query = time.time()
    result = oct1.groupby('event_type')['user_session'].nunique()
    query_time = time.time() - start_query

    print("Query results:")
    print(result)
    print(f"Query time: {query_time:.2f} s")


if __name__ == "__main__":
    main()