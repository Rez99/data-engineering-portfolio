from datetime import datetime

import boto3
import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


RUSTFS_ENDPOINT = "http://host.docker.internal:9000"
RUSTFS_ACCESS_KEY = "polaris_root"
RUSTFS_SECRET_KEY = "polaris_pass"
RUSTFS_BUCKET = "lakehouse-bucket"
OBJECT_KEY = "bronze/2019-Oct-sample.csv"

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=RUSTFS_ENDPOINT,
        aws_access_key_id=RUSTFS_ACCESS_KEY,
        aws_secret_access_key=RUSTFS_SECRET_KEY,
    )


def download_toy_data():
    import gzip

    url = "https://data.rees46.com/datasets/marketplace/2019-Oct.csv.gz"

    response = requests.get(
        url,
        stream=True,
        timeout=60,
    )
    response.raise_for_status()

    lines = []

    with gzip.GzipFile(fileobj=response.raw) as gz:
        for i, line in enumerate(gz):
            lines.append(line)

            # header + first 1000 data rows
            if i >= 1000:
                break

    csv_data = b"".join(lines)

    s3 = get_s3_client()

    s3.put_object(
        Bucket=RUSTFS_BUCKET,
        Key=OBJECT_KEY,
        Body=csv_data,
        ContentType="text/csv",
    )

    print(
        f"Uploaded {len(lines)} lines "
        f"to s3://{RUSTFS_BUCKET}/bronze/2019-Oct-sample.csv"
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
    dag_id="lakehouse_extract",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    download = PythonOperator(
        task_id="1_download_data",
        python_callable=download_toy_data,
    )

    validate = PythonOperator(
        task_id="2_validate_upload",
        python_callable=validate_upload,
    )

    download >> validate