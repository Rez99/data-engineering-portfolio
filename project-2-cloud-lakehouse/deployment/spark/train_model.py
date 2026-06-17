from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
from pathlib import Path


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


def ensure_dependencies() -> None:
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--quiet",
            "--disable-pip-version-check",
            "duckdb==1.3.1",
            "google-cloud-storage==3.1.1",
            "numpy==2.3.0",
            "pandas==2.3.0",
            "pyarrow==20.0.0",
            "scikit-learn==1.7.0",
            "xgboost==3.0.2",
        ],
        check=True,
    )


def download_parquet_files(
    client,
    bucket_name: str,
    prefix: str,
    destination: Path,
) -> list[Path]:
    from google.cloud import storage

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
    import pyarrow.dataset as ds

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
    import pyarrow.parquet as pq

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
    client,
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


def run(
    feature_bucket: str,
    feature_prefix: str,
    artifact_bucket: str,
    artifact_prefix: str,
) -> None:
    from google.cloud import storage

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
    parser = argparse.ArgumentParser(
        description="Train XGBoost from exported feature Parquet files."
    )
    parser.add_argument("--feature-bucket", required=True)
    parser.add_argument("--feature-prefix", required=True)
    parser.add_argument("--artifact-bucket", required=True)
    parser.add_argument("--artifact-prefix", required=True)
    args = parser.parse_args()

    ensure_dependencies()
    run(
        feature_bucket=args.feature_bucket,
        feature_prefix=args.feature_prefix,
        artifact_bucket=args.artifact_bucket,
        artifact_prefix=args.artifact_prefix,
    )
