from datetime import datetime

import boto3
import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


RUSTFS_ENDPOINT = "http://host.docker.internal:9000"
RUSTFS_ACCESS_KEY = "polaris_root"
RUSTFS_SECRET_KEY = "polaris_pass"
RUSTFS_BUCKET = "bucket123"
OBJECT_KEY = "jsonplaceholder/posts.json"


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=RUSTFS_ENDPOINT,
        aws_access_key_id=RUSTFS_ACCESS_KEY,
        aws_secret_access_key=RUSTFS_SECRET_KEY,
    )


def download_toy_data():
    url = "https://jsonplaceholder.typicode.com/posts"

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    s3 = get_s3_client()

    s3.put_object(
        Bucket=RUSTFS_BUCKET,
        Key=OBJECT_KEY,
        Body=response.text,
        ContentType="application/json",
    )

    print(
        f"Uploaded {len(response.text)} bytes "
        f"to s3://{RUSTFS_BUCKET}/{OBJECT_KEY}"
    )


def validate_upload():
    s3 = get_s3_client()

    response = s3.head_object(
        Bucket=RUSTFS_BUCKET,
        Key=OBJECT_KEY,
    )

    size = response["ContentLength"]

    if size == 0:
        raise ValueError("Uploaded object is empty")

    print(
        f"Validated s3://{RUSTFS_BUCKET}/{OBJECT_KEY} "
        f"({size} bytes)"
    )


with DAG(
    dag_id="download_toy_data",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    download = PythonOperator(
        task_id="download_toy_data",
        python_callable=download_toy_data,
    )

    validate = PythonOperator(
        task_id="validate_upload",
        python_callable=validate_upload,
    )

    download >> validate