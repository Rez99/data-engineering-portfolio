from datetime import datetime
import gzip
import os
import tempfile

import boto3
import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


RUSTFS_ENDPOINT = "http://host.docker.internal:9000"
RUSTFS_ACCESS_KEY = os.environ["RUSTFS_ACCESS_KEY"]
RUSTFS_SECRET_KEY = os.environ["RUSTFS_SECRET_KEY"]

RUSTFS_BUCKET = "lakehouse-bucket"
OBJECT_KEY = "bronze/raw-2019-Oct.csv.gz"

SOURCE_URL = "https://data.rees46.com/datasets/marketplace/2019-Oct.csv.gz"

# ------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------

DOWNLOAD_MODE = "full"  # "partial" or "full"
PARTIAL_MAX_LINES = 1000

# ------------------------------------------------------------------


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=RUSTFS_ENDPOINT,
        aws_access_key_id=RUSTFS_ACCESS_KEY,
        aws_secret_access_key=RUSTFS_SECRET_KEY,
    )


def extract_data():
    s3 = get_s3_client()

    if DOWNLOAD_MODE == "full":
        print("Downloading full source file...")

        with requests.get(
            SOURCE_URL,
            stream=True,
            timeout=60,
        ) as response:
            response.raise_for_status()

            s3.upload_fileobj(
                response.raw,
                RUSTFS_BUCKET,
                OBJECT_KEY,
            )

    elif DOWNLOAD_MODE == "partial":
        print(
            f"Downloading first {PARTIAL_MAX_LINES:,} lines..."
        )

        with tempfile.NamedTemporaryFile(
            mode="w+b",
            suffix=".csv.gz",
        ) as tmp:

            with requests.get(
                SOURCE_URL,
                stream=True,
                timeout=60,
            ) as response:
                response.raise_for_status()

                with gzip.GzipFile(
                    fileobj=response.raw,
                    mode="rb",
                ) as source_gz:

                    with gzip.GzipFile(
                        fileobj=tmp,
                        mode="wb",
                    ) as output_gz:

                        for i, line in enumerate(source_gz):
                            output_gz.write(line)

                            if i >= PARTIAL_MAX_LINES:
                                break

            tmp.flush()
            tmp.seek(0)

            s3.upload_fileobj(
                tmp,
                RUSTFS_BUCKET,
                OBJECT_KEY,
            )

    else:
        raise ValueError(
            f"Unknown DOWNLOAD_MODE: {DOWNLOAD_MODE}"
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
        f"Validated upload: "
        f"s3://{RUSTFS_BUCKET}/{OBJECT_KEY} "
        f"({size:,} bytes)"
    )


with DAG(
    dag_id="lakehouse_1_extract",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
) as dag:

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    validate = PythonOperator(
        task_id="validate_upload",
        python_callable=validate_upload,
    )

    extract >> validate