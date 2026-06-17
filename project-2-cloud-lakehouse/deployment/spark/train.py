import argparse
import shutil
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import xgboost as xgb
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    balanced_accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    roc_auc_score,
    roc_curve,
)


CATEGORY_COLUMNS = ["brand", "category_code"]
FEATURE_COLUMNS = [
    "brand",
    "category_code",
    "day_of_week",
    "hour_of_day",
    "view_count",
    "cart_add_count",
]
DEFAULT_BATCH_ROWS = 100_000
DEFAULT_BOOST_ROUNDS = 300
MAX_BIN = 256


class ParquetBatchIterator(xgb.DataIter):
    def __init__(
        self,
        parquet_path: Path,
        category_values: dict[str, list[str]],
        cache_prefix: Path,
        batch_rows: int,
    ):
        self.parquet_path = parquet_path
        self.category_values = category_values
        self.cache_prefix = cache_prefix
        self.batch_rows = batch_rows
        self.batch_iterator = None
        super().__init__(cache_prefix=str(cache_prefix))

    def next(self, input_data):
        if self.batch_iterator is None:
            self.reset()

        try:
            record_batch = next(self.batch_iterator)
        except StopIteration:
            return False

        batch = record_batch.to_pandas()
        labels = batch.pop("converted").astype(bool)
        apply_categories(batch, self.category_values)
        input_data(data=batch, label=labels)
        return True

    def reset(self):
        self.batch_iterator = pq.ParquetFile(self.parquet_path).iter_batches(
            batch_size=self.batch_rows
        )


def apply_categories(
    batch: pd.DataFrame,
    category_values: dict[str, list[str]],
) -> None:
    for column, categories in category_values.items():
        batch[column] = pd.Categorical(batch[column], categories=categories)


def write_parquet(frame: pd.DataFrame, path: Path) -> None:
    table = pa.Table.from_pandas(frame, preserve_index=False)
    pq.write_table(table, path, compression="zstd")


def prepare_training_data(
    feature_path: Path,
    train_path: Path,
    test_path: Path,
    batch_rows: int,
) -> None:
    selected_columns = ", ".join([*FEATURE_COLUMNS, "converted"])
    split_hash = (
        "hash(brand, category_code, day_of_week, hour_of_day, "
        "view_count, cart_add_count, converted) % 5"
    )

    connection = duckdb.connect()
    for path, predicate in (
        (train_path, f"{split_hash} != 0"),
        (test_path, f"{split_hash} = 0"),
    ):
        destination = str(path).replace("'", "''")
        connection.execute(
            f"""
            COPY (
                SELECT {selected_columns}
                FROM read_parquet(?)
                WHERE view_count > 0
                  AND {predicate}
            )
            TO '{destination}'
            (
                FORMAT parquet,
                COMPRESSION zstd,
                ROW_GROUP_SIZE {batch_rows}
            )
            """,
            [str(feature_path)],
        )

    train_rows = pq.ParquetFile(train_path).metadata.num_rows
    test_rows = pq.ParquetFile(test_path).metadata.num_rows
    if train_rows == 0 or test_rows == 0:
        raise ValueError(
            f"Train/test split produced {train_rows:,}/{test_rows:,} rows"
        )

    print(
        f"Prepared deterministic train/test split: "
        f"{train_rows:,}/{test_rows:,} rows"
    )


def get_category_values(feature_path: Path) -> dict[str, list[str]]:
    connection = duckdb.connect()
    category_values = {}
    for column in CATEGORY_COLUMNS:
        rows = connection.execute(
            f"""
            SELECT DISTINCT {column}
            FROM read_parquet(?)
            WHERE {column} IS NOT NULL
            ORDER BY {column}
            """,
            [str(feature_path)],
        ).fetchall()
        category_values[column] = [row[0] for row in rows]
    return category_values


def train_model(
    train_path: Path,
    model_path: Path,
    cache_dir: Path,
    category_values: dict[str, list[str]],
    batch_rows: int,
    boost_rounds: int,
) -> None:
    if not hasattr(xgb, "ExtMemQuantileDMatrix"):
        raise RuntimeError(
            "XGBoost 3.0 or newer is required for external-memory training"
        )

    shutil.rmtree(cache_dir, ignore_errors=True)
    cache_dir.mkdir(parents=True)

    iterator = ParquetBatchIterator(
        parquet_path=train_path,
        category_values=category_values,
        cache_prefix=cache_dir / "training",
        batch_rows=batch_rows,
    )
    training_data = xgb.ExtMemQuantileDMatrix(
        iterator,
        max_bin=MAX_BIN,
        enable_categorical=True,
    )
    model = xgb.train(
        params={
            "objective": "binary:logistic",
            "max_depth": 4,
            "eta": 0.05,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "seed": 42,
            "eval_metric": "logloss",
            "tree_method": "hist",
            "grow_policy": "depthwise",
            "max_bin": MAX_BIN,
        },
        dtrain=training_data,
        num_boost_round=boost_rounds,
    )
    model.save_model(model_path)
    print(f"Saved model: {model_path}")


def evaluate_model(
    test_path: Path,
    model_path: Path,
    output_dir: Path,
    category_values: dict[str, list[str]],
    batch_rows: int,
) -> None:
    model = xgb.Booster()
    model.load_model(model_path)

    labels = []
    probabilities = []
    for record_batch in pq.ParquetFile(test_path).iter_batches(
        batch_size=batch_rows
    ):
        batch = record_batch.to_pandas()
        labels.append(batch.pop("converted").astype(bool).to_numpy())
        apply_categories(batch, category_values)
        probabilities.append(
            model.predict(xgb.DMatrix(batch, enable_categorical=True))
        )

    y_test = np.concatenate(labels)
    y_prob = np.concatenate(probabilities)
    y_pred = y_prob >= 0.5

    auc = roc_auc_score(y_test, y_prob)
    accuracy = accuracy_score(y_test, y_pred)
    balanced_accuracy = balanced_accuracy_score(y_test, y_pred)
    true_f1 = f1_score(y_test, y_pred)
    pr_auc = average_precision_score(y_test, y_prob)
    report = classification_report(
        y_test,
        y_pred,
        output_dict=True,
        zero_division=0,
    )
    matrix = confusion_matrix(y_test, y_pred, labels=[False, True])
    fpr, tpr, thresholds = roc_curve(y_test, y_prob)

    baseline_pred = np.zeros_like(y_test, dtype=bool)
    baseline_prob = np.zeros_like(y_prob)
    model_comparison = pd.DataFrame(
        [
            {
                "model": "Always predict False",
                "accuracy": accuracy_score(y_test, baseline_pred),
                "balanced_accuracy": balanced_accuracy_score(
                    y_test, baseline_pred
                ),
                "true_class_f1": f1_score(
                    y_test, baseline_pred, zero_division=0
                ),
                "roc_auc": roc_auc_score(y_test, baseline_prob),
                "pr_auc": average_precision_score(y_test, baseline_prob),
            },
            {
                "model": "XGBoost",
                "accuracy": accuracy,
                "balanced_accuracy": balanced_accuracy,
                "true_class_f1": true_f1,
                "roc_auc": auc,
                "pr_auc": pr_auc,
            },
        ]
    )

    importance = model.get_score(importance_type="gain")
    total_importance = sum(importance.values())
    feature_importance = pd.DataFrame(
        {
            "feature": FEATURE_COLUMNS,
            "importance": [
                (
                    importance.get(feature, 0.0) / total_importance
                    if total_importance
                    else 0.0
                )
                for feature in FEATURE_COLUMNS
            ],
        }
    ).sort_values("importance", ascending=False)

    metrics = pd.DataFrame(
        [
            {"metric": "roc_auc", "value": auc},
            {"metric": "accuracy", "value": accuracy},
            {
                "metric": "precision_false",
                "value": report["False"]["precision"],
            },
            {"metric": "recall_false", "value": report["False"]["recall"]},
            {"metric": "f1_false", "value": report["False"]["f1-score"]},
            {
                "metric": "precision_true",
                "value": report["True"]["precision"],
            },
            {"metric": "recall_true", "value": report["True"]["recall"]},
            {"metric": "f1_true", "value": report["True"]["f1-score"]},
            {"metric": "macro_f1", "value": report["macro avg"]["f1-score"]},
            {
                "metric": "weighted_f1",
                "value": report["weighted avg"]["f1-score"],
            },
        ]
    )
    confusion = pd.DataFrame(
        [
            {
                "actual": "False",
                "predicted": "False",
                "count": int(matrix[0, 0]),
            },
            {
                "actual": "False",
                "predicted": "True",
                "count": int(matrix[0, 1]),
            },
            {
                "actual": "True",
                "predicted": "False",
                "count": int(matrix[1, 0]),
            },
            {
                "actual": "True",
                "predicted": "True",
                "count": int(matrix[1, 1]),
            },
        ]
    )
    roc_points = pd.DataFrame(
        {"fpr": fpr, "tpr": tpr, "threshold": thresholds}
    )

    write_parquet(metrics, output_dir / "metrics.parquet")
    write_parquet(model_comparison, output_dir / "model_comparison.parquet")
    write_parquet(confusion, output_dir / "confusion_matrix.parquet")
    write_parquet(feature_importance, output_dir / "feature_importance.parquet")
    write_parquet(roc_points, output_dir / "roc_curve.parquet")

    print(model_comparison.to_string(index=False))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train and evaluate the ecommerce conversion model."
    )
    parser.add_argument("--features", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--batch-rows", type=int, default=DEFAULT_BATCH_ROWS)
    parser.add_argument(
        "--boost-rounds",
        type=int,
        default=DEFAULT_BOOST_ROUNDS,
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    train_path = args.output_dir / "train_data.parquet"
    test_path = args.output_dir / "test_data.parquet"
    model_path = args.output_dir / "xgboost_model.json"
    cache_dir = args.output_dir / "xgboost_cache"

    prepare_training_data(
        args.features,
        train_path,
        test_path,
        args.batch_rows,
    )
    category_values = get_category_values(args.features)
    train_model(
        train_path,
        model_path,
        cache_dir,
        category_values,
        args.batch_rows,
        args.boost_rounds,
    )
    evaluate_model(
        test_path,
        model_path,
        args.output_dir,
        category_values,
        args.batch_rows,
    )


if __name__ == "__main__":
    main()
