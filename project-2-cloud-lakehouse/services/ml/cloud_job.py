import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pyarrow.dataset as ds
import pyarrow.parquet as pq
from google.cloud import storage


EXPECTED_COLUMNS = {
    "brand",
    "category_code",
    "day_of_week",
    "hour_of_day",
    "view_count",
    "cart_add_count",
    "converted",
}
ARTIFACT_PATHS = {
    "xgboost_model.json": "model/xgboost_model.json",
    "metrics.parquet": "metrics/metrics.parquet",
    "model_comparison.parquet": "metrics/model_comparison.parquet",
    "confusion_matrix.parquet": "metrics/confusion_matrix.parquet",
    "feature_importance.parquet": "metrics/feature_importance.parquet",
    "roc_curve.parquet": "metrics/roc_curve.parquet",
}


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Required environment variable is not set: {name}")
    return value


def download_parquet_files(
    client: storage.Client,
    bucket_name: str,
    prefix: str,
    destination: Path,
) -> list[Path]:
    paths = []
    for blob in client.list_blobs(bucket_name, prefix=prefix):
        if not blob.name.endswith(".parquet"):
            continue

        path = destination / Path(blob.name).name
        blob.download_to_filename(path)
        paths.append(path)

    if not paths:
        raise ValueError(f"No Parquet files found at gs://{bucket_name}/{prefix}")
    return paths


def validate_features(paths: list[Path]) -> dict[str, object]:
    dataset = ds.dataset([str(path) for path in paths], format="parquet")
    columns = set(dataset.schema.names)
    missing = EXPECTED_COLUMNS - columns
    if missing:
        raise ValueError(f"Feature export is missing columns: {sorted(missing)}")

    row_count = dataset.count_rows()
    if row_count == 0:
        raise ValueError("Feature export contains no rows")

    return {
        "status": "ready",
        "row_count": row_count,
        "columns": sorted(columns),
        "parquet_files": len(paths),
    }


def merge_parquet_files(paths: list[Path], destination: Path) -> None:
    writer = None
    try:
        for path in paths:
            parquet = pq.ParquetFile(path)
            for batch in parquet.iter_batches():
                if writer is None:
                    writer = pq.ParquetWriter(
                        destination,
                        batch.schema,
                        compression="zstd",
                    )
                writer.write_batch(batch)
    finally:
        if writer is not None:
            writer.close()

    if writer is None:
        raise ValueError("Feature export contained no Parquet record batches")


def upload_artifacts(
    client: storage.Client,
    bucket_name: str,
    prefix: str,
    output_dir: Path,
) -> None:
    bucket = client.bucket(bucket_name)
    for filename, relative_object in ARTIFACT_PATHS.items():
        path = output_dir / filename
        if not path.exists() or path.stat().st_size == 0:
            raise ValueError(f"Training artifact is missing or empty: {filename}")

        object_name = f"{prefix.rstrip('/')}/{relative_object}"
        bucket.blob(object_name).upload_from_filename(path)
        print(f"Uploaded gs://{bucket_name}/{object_name}")


def run() -> None:
    feature_bucket = required_env("FEATURE_BUCKET")
    feature_prefix = required_env("FEATURE_PREFIX")
    artifact_bucket = required_env("ARTIFACT_BUCKET")
    artifact_prefix = required_env("ARTIFACT_PREFIX")

    client = storage.Client()
    with tempfile.TemporaryDirectory(prefix="lakehouse-ml-") as temp_dir:
        temp_path = Path(temp_dir)
        paths = download_parquet_files(
            client,
            feature_bucket,
            feature_prefix,
            temp_path,
        )
        result = validate_features(paths)
        feature_path = temp_path / "features.parquet"
        output_dir = temp_path / "output"
        merge_parquet_files(paths, feature_path)

        print(
            f"Training from {result['row_count']:,} feature rows across "
            f"{result['parquet_files']} Parquet files"
        )
        subprocess.run(
            [
                sys.executable,
                str(Path(__file__).with_name("train.py")),
                "--features",
                str(feature_path),
                "--output-dir",
                str(output_dir),
            ],
            check=True,
        )
        upload_artifacts(
            client,
            artifact_bucket,
            artifact_prefix,
            output_dir,
        )


if __name__ == "__main__":
    run()
