from datetime import datetime
import os

import duckdb
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


POLARIS_URI = "http://host.docker.internal:8181/api/catalog"
POLARIS_CLIENT_ID = os.environ["AIRFLOW_CLIENT_ID"]
POLARIS_CLIENT_SECRET = os.environ["AIRFLOW_CLIENT_SECRET"]

RUSTFS_ENDPOINT = "host.docker.internal:9000"

SCHEMA_NAME = "bronze"
TABLE_NAME = "iceberg-2019-Oct"
TABLE_FQN = f'polaris.{SCHEMA_NAME}."{TABLE_NAME}"'

PARQUET_S3 = "s3://lakehouse-bucket/bronze/raw-2019-Oct.parquet"
CSV_GZ_S3 = "s3://lakehouse-bucket/bronze/raw-2019-Oct.csv.gz"


def get_connection():
    con = duckdb.connect()

    con.execute("""
        INSTALL iceberg;
        LOAD iceberg;

        INSTALL httpfs;
        LOAD httpfs;
    """)

    con.execute(f"""
        CREATE OR REPLACE SECRET rustfs_secret (
            TYPE S3,
            KEY_ID 'polaris_root',
            SECRET 'polaris_pass',
            ENDPOINT '{RUSTFS_ENDPOINT}',
            URL_STYLE 'path',
            USE_SSL false
        );
    """)

    return con


def get_connection_with_polaris():
    con = get_connection()

    con.execute(f"""
        ATTACH 'lakehouse' AS polaris (
            TYPE ICEBERG,
            ENDPOINT '{POLARIS_URI}',
            CLIENT_ID '{POLARIS_CLIENT_ID}',
            CLIENT_SECRET '{POLARIS_CLIENT_SECRET}'
        );
    """)

    return con


def ensure_schema():
    con = get_connection_with_polaris()

    con.execute(f"""
        CREATE SCHEMA IF NOT EXISTS polaris.{SCHEMA_NAME}
    """)

    print(f"Ensured schema polaris.{SCHEMA_NAME}")


def drop_table():
    con = get_connection_with_polaris()

    con.execute(f"""
        DROP TABLE IF EXISTS {TABLE_FQN}
    """)

    print(f"Dropped {TABLE_FQN}")


def csv_to_parquet():
    con = get_connection()

    con.execute(f"""
        COPY (
            SELECT *
            FROM read_csv_auto('{CSV_GZ_S3}')
        )
        TO '{PARQUET_S3}'
        (
            FORMAT parquet,
            ROW_GROUP_SIZE 100000
        )
    """)

    print(f"Wrote {PARQUET_S3}")


def parquet_to_iceberg():
    con = get_connection_with_polaris()

    print(f"Creating {TABLE_FQN}")

    con.execute(f"""
        CREATE TABLE {TABLE_FQN} AS
        SELECT *
        FROM read_parquet('{PARQUET_S3}')
    """)

    print(f"Finished loading {TABLE_FQN}")


def validate_table():
    con = get_connection_with_polaris()

    count = con.execute(f"""
        SELECT COUNT(*)
        FROM {TABLE_FQN}
    """).fetchone()[0]

    print(f"Row count = {count:,}")

    if count == 0:
        raise ValueError("Iceberg table is empty")


with DAG(
    dag_id="lakehouse_2_load",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
) as dag:

    t_ensure_schema = PythonOperator(
        task_id="1_ensure_schema",
        python_callable=ensure_schema,
    )

    t_drop_table = PythonOperator(
        task_id="2_drop_table",
        python_callable=drop_table,
    )

    t_csv_to_parquet = PythonOperator(
        task_id="3_csv_to_parquet",
        python_callable=csv_to_parquet,
    )

    t_parquet_to_iceberg = PythonOperator(
        task_id="4_parquet_to_iceberg",
        python_callable=parquet_to_iceberg,
    )

    t_validate = PythonOperator(
        task_id="5_validate",
        python_callable=validate_table,
    )

    (
        t_ensure_schema
        >> t_drop_table
        >> t_csv_to_parquet
        >> t_parquet_to_iceberg
        >> t_validate
    )