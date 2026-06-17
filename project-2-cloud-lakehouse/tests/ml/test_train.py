import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


EXPECTED_ARTIFACTS = [
    "xgboost_model.json",
    "metrics.parquet",
    "model_comparison.parquet",
    "confusion_matrix.parquet",
    "feature_importance.parquet",
    "roc_curve.parquet",
]

ML_SOURCE_DIR = (
    Path(__file__).resolve().parents[2] / "deployment" / "spark"
)


class TrainingTest(unittest.TestCase):
    def test_trains_and_writes_evaluation_artifacts(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            feature_path = root / "features.parquet"
            output_dir = root / "output"

            rows = []
            for index in range(500):
                cart_count = index % 4
                rows.append(
                    {
                        "brand": f"brand-{index % 5}",
                        "category_code": f"category-{index % 3}",
                        "day_of_week": index % 7,
                        "hour_of_day": index % 24,
                        "view_count": 1 + index % 8,
                        "cart_add_count": cart_count,
                        "converted": int(
                            cart_count >= 2 and index % 5 != 0
                        ),
                    }
                )

            frame = pd.DataFrame(rows)
            pq.write_table(
                pa.Table.from_pandas(frame, preserve_index=False),
                feature_path,
            )

            subprocess.run(
                [
                    sys.executable,
                    "train.py",
                    "--features",
                    str(feature_path),
                    "--output-dir",
                    str(output_dir),
                    "--batch-rows",
                    "64",
                    "--boost-rounds",
                    "5",
                ],
                cwd=ML_SOURCE_DIR,
                check=True,
            )

            for artifact in EXPECTED_ARTIFACTS:
                artifact_path = output_dir / artifact
                self.assertTrue(artifact_path.exists(), artifact)
                self.assertGreater(artifact_path.stat().st_size, 0, artifact)

            comparison = pq.read_table(
                output_dir / "model_comparison.parquet"
            ).to_pandas()
            self.assertEqual(
                comparison["model"].tolist(),
                ["Always predict False", "XGBoost"],
            )


if __name__ == "__main__":
    unittest.main()
