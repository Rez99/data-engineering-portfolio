import tempfile
import unittest
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from cloud_job import (
    ARTIFACT_PATHS,
    merge_parquet_files,
    upload_artifacts,
    validate_features,
)


class FakeBlob:
    def __init__(self, name):
        self.name = name
        self.uploaded_path = None

    def upload_from_filename(self, path):
        self.uploaded_path = path


class FakeBucket:
    def __init__(self):
        self.blobs = {}

    def blob(self, name):
        blob = FakeBlob(name)
        self.blobs[name] = blob
        return blob


class FakeClient:
    def __init__(self):
        self.buckets = {}

    def bucket(self, name):
        return self.buckets.setdefault(name, FakeBucket())


class CloudJobTest(unittest.TestCase):
    def test_validates_exported_feature_files(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "features.parquet"
            frame = pd.DataFrame(
                [
                    {
                        "brand": "brand-a",
                        "category_code": "category-a",
                        "day_of_week": 1,
                        "hour_of_day": 12,
                        "view_count": 3,
                        "cart_add_count": 1,
                        "converted": 0,
                    },
                    {
                        "brand": "brand-b",
                        "category_code": "category-b",
                        "day_of_week": 2,
                        "hour_of_day": 13,
                        "view_count": 4,
                        "cart_add_count": 2,
                        "converted": 1,
                    },
                ]
            )
            pq.write_table(
                pa.Table.from_pandas(frame, preserve_index=False),
                path,
            )

            result = validate_features([path])

            self.assertEqual(result["status"], "ready")
            self.assertEqual(result["row_count"], 2)
            self.assertEqual(result["parquet_files"], 1)

    def test_rejects_missing_columns(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "features.parquet"
            pq.write_table(
                pa.table({"brand": ["brand-a"]}),
                path,
            )

            with self.assertRaisesRegex(ValueError, "missing columns"):
                validate_features([path])

    def test_merges_parquet_parts_without_loading_the_full_dataset(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            paths = []
            for index in range(2):
                path = root / f"part-{index}.parquet"
                pq.write_table(
                    pa.table(
                        {
                            "brand": [f"brand-{index}"],
                            "category_code": ["category"],
                            "day_of_week": [index],
                            "hour_of_day": [12],
                            "view_count": [3],
                            "cart_add_count": [1],
                            "converted": [index],
                        }
                    ),
                    path,
                )
                paths.append(path)

            merged = root / "features.parquet"
            merge_parquet_files(paths, merged)

            self.assertEqual(pq.ParquetFile(merged).metadata.num_rows, 2)

    def test_uploads_only_production_artifacts(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            for filename in ARTIFACT_PATHS:
                (output_dir / filename).write_bytes(b"artifact")

            client = FakeClient()
            upload_artifacts(
                client,
                "lakehouse-bucket",
                "ml/xgboost_conversion",
                output_dir,
            )

            bucket = client.buckets["lakehouse-bucket"]
            self.assertEqual(
                set(bucket.blobs),
                {
                    f"ml/xgboost_conversion/{path}"
                    for path in ARTIFACT_PATHS.values()
                },
            )


if __name__ == "__main__":
    unittest.main()
