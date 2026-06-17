import argparse
import csv
import gzip
import io
import subprocess
import sys


def ensure_dependencies() -> None:
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--quiet",
            "--disable-pip-version-check",
            "google-cloud-storage==3.1.1",
            "requests==2.32.4",
        ],
        check=True,
    )


EXPECTED_COLUMNS = [
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
]


def copy_sample(source, destination, max_rows):
    header = source.readline()
    if not header:
        raise ValueError("Source dataset is empty")

    destination.write(header)

    rows_written = 0
    for _ in range(max_rows):
        row = source.readline()
        if not row:
            break

        destination.write(row)
        rows_written += 1

    if rows_written != max_rows:
        raise ValueError(
            f"Source contained {rows_written:,} data rows; expected {max_rows:,}"
        )

    return rows_written


def validate_sample(source, expected_rows, expected_columns):
    magic = source.read(2)
    if magic != b"\x1f\x8b":
        raise ValueError("Extracted object is not gzip-compressed")
    source.seek(0)

    with gzip.GzipFile(fileobj=source, mode="rb") as compressed:
        with io.TextIOWrapper(compressed, encoding="utf-8", newline="") as text:
            reader = csv.reader(text)
            header = next(reader, None)

            if header != expected_columns:
                raise ValueError(
                    f"Unexpected columns: {header}; expected {expected_columns}"
                )

            rows_read = sum(1 for _ in reader)

    if rows_read != expected_rows:
        raise ValueError(
            f"Extracted object contained {rows_read:,} rows; "
            f"expected {expected_rows:,}"
        )

    return rows_read


def extract(source_url, destination_bucket, destination_object, max_rows):
    from google.cloud import storage
    import requests

    if max_rows < 1:
        raise ValueError("MAX_ROWS must be greater than zero")

    client = storage.Client()
    blob = client.bucket(destination_bucket).blob(destination_object)
    blob.content_type = "application/gzip"

    print(f"Streaming {max_rows:,} rows from {source_url}")

    with requests.get(source_url, stream=True, timeout=(30, 300)) as response:
        response.raise_for_status()
        response.raw.decode_content = False

        with gzip.GzipFile(fileobj=response.raw, mode="rb") as source:
            with blob.open("wb", chunk_size=8 * 1024 * 1024) as cloud_writer:
                with gzip.GzipFile(fileobj=cloud_writer, mode="wb") as destination:
                    rows_written = copy_sample(source, destination, max_rows)

    print(
        f"Wrote {rows_written:,} rows to "
        f"gs://{destination_bucket}/{destination_object}"
    )

    print("Validating uploaded gzip, CSV schema, and row count")
    with blob.open("rb") as cloud_reader:
        rows_validated = validate_sample(
            cloud_reader,
            expected_rows=max_rows,
            expected_columns=EXPECTED_COLUMNS,
        )

    print(
        f"Validated {rows_validated:,} rows and "
        f"{len(EXPECTED_COLUMNS)} columns"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract a bounded ecommerce clickstream sample into GCS."
    )
    parser.add_argument("--source-url", required=True)
    parser.add_argument("--destination-bucket", required=True)
    parser.add_argument("--destination-object", required=True)
    parser.add_argument("--max-rows", type=int, required=True)
    args = parser.parse_args()

    ensure_dependencies()
    extract(
        source_url=args.source_url,
        destination_bucket=args.destination_bucket,
        destination_object=args.destination_object,
        max_rows=args.max_rows,
    )
