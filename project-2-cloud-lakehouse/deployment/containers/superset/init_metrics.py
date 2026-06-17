import os
from pathlib import Path

import duckdb
from google.cloud import storage


DATABASE_PATH = Path("/tmp/ml_metrics.duckdb")
DATA_DIRECTORY = Path("/tmp/ml_metrics")
VIEW_NAMES = (
    "metrics",
    "model_comparison",
    "confusion_matrix",
    "feature_importance",
    "roc_curve",
)


def sql_string(value: str) -> str:
    return value.replace("'", "''")


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Required environment variable is not set: {name}")
    return value


def main() -> None:
    bucket_name = required_env("METRICS_BUCKET")
    prefix = required_env("METRICS_PREFIX").strip("/")
    DATA_DIRECTORY.mkdir(parents=True, exist_ok=True)

    bucket = storage.Client().bucket(bucket_name)
    for view_name in VIEW_NAMES:
        object_name = f"{prefix}/{view_name}.parquet"
        destination = DATA_DIRECTORY / f"{view_name}.parquet"
        bucket.blob(object_name).download_to_filename(destination)

    with duckdb.connect(str(DATABASE_PATH)) as connection:
        connection.execute("CREATE SCHEMA IF NOT EXISTS ml")
        for view_name in VIEW_NAMES:
            parquet_path = DATA_DIRECTORY / f"{view_name}.parquet"
            connection.execute(
                f"""
                CREATE OR REPLACE VIEW ml.{view_name} AS
                SELECT *
                FROM read_parquet('{sql_string(str(parquet_path))}')
                """
            )

        for view_name in VIEW_NAMES:
            row_count = connection.execute(
                f"SELECT count(*) FROM ml.{view_name}"
            ).fetchone()[0]
            print(f"Validated ml.{view_name}: {row_count:,} rows")


if __name__ == "__main__":
    main()
