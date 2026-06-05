from datetime import datetime
import os
import duckdb
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


POLARIS_URI = "http://host.docker.internal:8181/api/catalog"
POLARIS_CLIENT_ID = os.environ["AIRFLOW_CLIENT_ID"]
POLARIS_CLIENT_SECRET = os.environ["AIRFLOW_CLIENT_SECRET"]
RUSTFS_ENDPOINT = "host.docker.internal:9000"
TABLE_NAME = "events_sample"


def get_connection():
    con = duckdb.connect()
    con.execute("""
        INSTALL iceberg; LOAD iceberg;
        INSTALL httpfs;  LOAD httpfs;
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
    con = get_connection()
    con.execute("CREATE SCHEMA IF NOT EXISTS polaris.silver")


def drop_table():
    con = get_connection()
    con.execute(f"DROP TABLE IF EXISTS polaris.silver.{TABLE_NAME}")


def load_table():
    con = get_connection()
    con.execute(f"""
        CREATE TABLE polaris.silver.{TABLE_NAME} AS
        SELECT *
        FROM read_csv_auto('s3://lakehouse-bucket/bronze/2019-Oct-sample.csv')
    """)


def validate_table():
    con = get_connection()
    count = con.execute(f"SELECT COUNT(*) FROM polaris.silver.{TABLE_NAME}").fetchone()[0]
    if count == 0:
        raise ValueError("Iceberg table is empty")
    print(f"Validated {count} rows")


with DAG(
    dag_id="lakehouse_load",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    t_ensure_schema = PythonOperator(task_id="1_ensure_schema", python_callable=ensure_schema)
    t_drop_table    = PythonOperator(task_id="2_drop_table",    python_callable=drop_table)
    t_load_table    = PythonOperator(task_id="3_load_table",    python_callable=load_table)
    t_validate      = PythonOperator(task_id="4_validate",      python_callable=validate_table)

    t_ensure_schema >> t_drop_table >> t_load_table >> t_validate