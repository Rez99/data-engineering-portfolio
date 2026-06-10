from datetime import datetime
import os
import shutil

import boto3
import duckdb
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import xgboost as xgb
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    roc_auc_score,
    roc_curve,
)


POLARIS_URI = "http://host.docker.internal:8181/api/catalog"

RUSTFS_ENDPOINT = "http://host.docker.internal:9000"
RUSTFS_ACCESS_KEY = os.environ["RUSTFS_ACCESS_KEY"]
RUSTFS_SECRET_KEY = os.environ["RUSTFS_SECRET_KEY"]

RUSTFS_BUCKET = "lakehouse-bucket"

LOCAL_DIR = "/opt/airflow/data/ml"
FEATURE_STORE_PATH = f"{LOCAL_DIR}/mart_session.parquet"
MODEL_PATH = f"{LOCAL_DIR}/xgboost_model.json"
TRAIN_DATA_PATH = f"{LOCAL_DIR}/train_data.parquet"
TEST_DATA_PATH = f"{LOCAL_DIR}/test_data.parquet"
XGBOOST_CACHE_DIR = f"{LOCAL_DIR}/xgboost_cache"

METRICS_PATH = f"{LOCAL_DIR}/metrics.parquet"
CONFUSION_MATRIX_PATH = f"{LOCAL_DIR}/confusion_matrix.parquet"
FEATURE_IMPORTANCE_PATH = f"{LOCAL_DIR}/feature_importance.parquet"
ROC_CURVE_PATH = f"{LOCAL_DIR}/roc_curve.parquet"

ML_OBJECT_PREFIX = "ml/xgboost_conversion"
TRAIN_BATCH_ROWS = 100_000
MAX_BIN = 256
CATEGORY_COLUMNS = ["brand", "category_code"]
FEATURE_COLUMNS = [
    "brand",
    "category_code",
    "day_of_week",
    "hour_of_day",
    "view_count",
    "cart_add_count",
]


class ParquetBatchIterator(xgb.DataIter):
    def __init__(self, parquet_path, category_values, cache_prefix):
        self.parquet_path = parquet_path
        self.category_values = category_values
        self.batch_iterator = None
        super().__init__(cache_prefix=cache_prefix)

    def next(self, input_data):
        if self.batch_iterator is None:
            self.reset()

        try:
            record_batch = next(self.batch_iterator)
        except StopIteration:
            return False

        batch = record_batch.to_pandas()
        labels = batch.pop("converted").astype(bool)

        for column, categories in self.category_values.items():
            batch[column] = pd.Categorical(batch[column], categories=categories)

        input_data(data=batch, label=labels)
        return True

    def reset(self):
        self.batch_iterator = pq.ParquetFile(self.parquet_path).iter_batches(
            batch_size=TRAIN_BATCH_ROWS
        )


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=RUSTFS_ENDPOINT,
        aws_access_key_id=RUSTFS_ACCESS_KEY,
        aws_secret_access_key=RUSTFS_SECRET_KEY,
    )


def get_polaris_connection():
    con = duckdb.connect()
    con.execute(f"""
        SET temp_directory = '{LOCAL_DIR}/duckdb_temp';
        SET memory_limit = '2GB';
    """)

    con.execute("""
        INSTALL iceberg;
        LOAD iceberg;

        INSTALL httpfs;
        LOAD httpfs;
    """)

    con.execute(f"""
        ATTACH 'lakehouse' AS polaris (
            TYPE ICEBERG,
            ENDPOINT '{POLARIS_URI}',
            CLIENT_ID '{os.environ["AIRFLOW_CLIENT_ID"]}',
            CLIENT_SECRET '{os.environ["AIRFLOW_CLIENT_SECRET"]}'
        );
    """)

    return con


def write_parquet(df, path):
    con = duckdb.connect()
    con.register("df_to_write", df)

    con.execute(f"""
        COPY df_to_write
        TO '{path}'
        (
            FORMAT parquet
        )
    """)


def load_feature_store():
    os.makedirs(LOCAL_DIR, exist_ok=True)

    con = get_polaris_connection()

    if os.path.exists(FEATURE_STORE_PATH):
        os.remove(FEATURE_STORE_PATH)

    con.execute(f"""
        COPY (
            SELECT
                brand,
                category_code,
                day_of_week,
                hour_of_day,
                view_count,
                cart_add_count,
                converted
            FROM polaris.gold.mart_session
            WHERE view_count > 0
        )
        TO '{FEATURE_STORE_PATH}'
        (
            FORMAT parquet,
            COMPRESSION zstd,
            ROW_GROUP_SIZE {TRAIN_BATCH_ROWS}
        )
    """)

    distribution = con.execute(f"""
        SELECT
            converted,
            count(*) AS session_count,
            count(*)::DOUBLE / sum(count(*)) OVER () AS proportion
        FROM read_parquet('{FEATURE_STORE_PATH}')
        GROUP BY converted
        ORDER BY converted
    """).fetchdf()

    session_count = int(distribution["session_count"].sum())
    print(f"Loaded {session_count:,} sessions")
    print()
    print("Target distribution:")
    print(distribution.to_string(index=False))

    print(f"Wrote local feature store: {FEATURE_STORE_PATH}")


def prepare_training_data():
    for path in (TRAIN_DATA_PATH, TEST_DATA_PATH):
        if os.path.exists(path):
            os.remove(path)

    con = duckdb.connect()
    con.execute(f"""
        SET temp_directory = '{LOCAL_DIR}/duckdb_temp';
        SET memory_limit = '2GB';
    """)

    split_hash = """
        hash(
            brand,
            category_code,
            day_of_week,
            hour_of_day,
            view_count,
            cart_add_count,
            converted
        ) % 5
    """

    for path, predicate in (
        (TRAIN_DATA_PATH, f"{split_hash} != 0"),
        (TEST_DATA_PATH, f"{split_hash} = 0"),
    ):
        con.execute(f"""
            COPY (
                SELECT *
                FROM read_parquet('{FEATURE_STORE_PATH}')
                WHERE {predicate}
            )
            TO '{path}'
            (
                FORMAT parquet,
                COMPRESSION zstd,
                ROW_GROUP_SIZE {TRAIN_BATCH_ROWS}
            )
        """)

    split_counts = con.execute(f"""
        SELECT 'train' AS split, count(*) AS row_count
        FROM read_parquet('{TRAIN_DATA_PATH}')
        UNION ALL
        SELECT 'test' AS split, count(*) AS row_count
        FROM read_parquet('{TEST_DATA_PATH}')
    """).fetchdf()

    print("Materialized deterministic train/test split:")
    print(split_counts.to_string(index=False))


def get_category_values():
    con = duckdb.connect()
    category_values = {}

    for column in CATEGORY_COLUMNS:
        rows = con.execute(f"""
            SELECT DISTINCT {column}
            FROM read_parquet('{FEATURE_STORE_PATH}')
            WHERE {column} IS NOT NULL
            ORDER BY {column}
        """).fetchall()
        category_values[column] = [row[0] for row in rows]

    return category_values


def train_model():
    if not hasattr(xgb, "ExtMemQuantileDMatrix"):
        raise RuntimeError(
            "XGBoost 3.0 or newer is required for external-memory training"
        )

    prepare_training_data()
    category_values = get_category_values()

    shutil.rmtree(XGBOOST_CACHE_DIR, ignore_errors=True)
    os.makedirs(XGBOOST_CACHE_DIR, exist_ok=True)

    training_rows = pq.ParquetFile(TRAIN_DATA_PATH).metadata.num_rows
    if training_rows == 0:
        raise ValueError("No training rows were available for XGBoost")

    iterator = ParquetBatchIterator(
        parquet_path=TRAIN_DATA_PATH,
        category_values=category_values,
        cache_prefix=f"{XGBOOST_CACHE_DIR}/training",
    )
    dtrain = xgb.ExtMemQuantileDMatrix(
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
        dtrain=dtrain,
        num_boost_round=300,
    )

    model.save_model(MODEL_PATH)

    print(f"Saved local model: {MODEL_PATH}")
    print(
        f"Trained on {training_rows:,} rows from streamed Parquet batches "
        f"cached under {XGBOOST_CACHE_DIR}"
    )


def evaluate_model():
    model = xgb.Booster()
    model.load_model(MODEL_PATH)

    category_values = get_category_values()
    labels = []
    probabilities = []

    for record_batch in pq.ParquetFile(TEST_DATA_PATH).iter_batches(
        batch_size=TRAIN_BATCH_ROWS
    ):
        batch = record_batch.to_pandas()
        labels.append(batch.pop("converted").astype(bool).to_numpy())

        for column, categories in category_values.items():
            batch[column] = pd.Categorical(batch[column], categories=categories)

        dtest = xgb.DMatrix(batch, enable_categorical=True)
        probabilities.append(model.predict(dtest))

    if not labels:
        raise ValueError("No test rows were available for evaluation")

    y_test = np.concatenate(labels)
    y_prob = np.concatenate(probabilities)
    y_pred = y_prob >= 0.5

    auc = roc_auc_score(y_test, y_prob)
    accuracy = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred, output_dict=True)
    cm = confusion_matrix(y_test, y_pred)
    fpr, tpr, thresholds = roc_curve(y_test, y_prob)

    print("\nROC AUC")
    print("-------")
    print(f"{auc:.4f}")

    print("\nClassification Report")
    print("---------------------")
    print(classification_report(y_test, y_pred))

    print("\nConfusion Matrix")
    print("----------------")
    print(cm)

    importance = model.get_score(importance_type="gain")
    total_importance = sum(importance.values())
    feature_importance_df = pd.DataFrame(
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

    print("\nFeature Importance")
    print("------------------")
    print(feature_importance_df.to_string(index=False))

    metrics_df = pd.DataFrame(
        [
            {"metric": "roc_auc", "value": auc},
            {"metric": "accuracy", "value": accuracy},
            {"metric": "precision_false", "value": report["False"]["precision"]},
            {"metric": "recall_false", "value": report["False"]["recall"]},
            {"metric": "f1_false", "value": report["False"]["f1-score"]},
            {"metric": "precision_true", "value": report["True"]["precision"]},
            {"metric": "recall_true", "value": report["True"]["recall"]},
            {"metric": "f1_true", "value": report["True"]["f1-score"]},
            {"metric": "macro_f1", "value": report["macro avg"]["f1-score"]},
            {"metric": "weighted_f1", "value": report["weighted avg"]["f1-score"]},
        ]
    )

    confusion_matrix_df = pd.DataFrame(
        [
            {"actual": "False", "predicted": "False", "count": int(cm[0, 0])},
            {"actual": "False", "predicted": "True", "count": int(cm[0, 1])},
            {"actual": "True", "predicted": "False", "count": int(cm[1, 0])},
            {"actual": "True", "predicted": "True", "count": int(cm[1, 1])},
        ]
    )

    roc_curve_df = pd.DataFrame(
        {
            "fpr": fpr,
            "tpr": tpr,
            "threshold": thresholds,
        }
    )

    write_parquet(metrics_df, METRICS_PATH)
    write_parquet(confusion_matrix_df, CONFUSION_MATRIX_PATH)
    write_parquet(feature_importance_df, FEATURE_IMPORTANCE_PATH)
    write_parquet(roc_curve_df, ROC_CURVE_PATH)

    print(f"Wrote local metrics: {METRICS_PATH}")
    print(f"Wrote local confusion matrix: {CONFUSION_MATRIX_PATH}")
    print(f"Wrote local feature importance: {FEATURE_IMPORTANCE_PATH}")
    print(f"Wrote local ROC curve: {ROC_CURVE_PATH}")


def write_artifacts_to_rustfs():
    s3 = get_s3_client()

    artifacts = {
        MODEL_PATH: f"{ML_OBJECT_PREFIX}/model/xgboost_model.json",
        METRICS_PATH: f"{ML_OBJECT_PREFIX}/metrics/metrics.parquet",
        CONFUSION_MATRIX_PATH: f"{ML_OBJECT_PREFIX}/metrics/confusion_matrix.parquet",
        FEATURE_IMPORTANCE_PATH: f"{ML_OBJECT_PREFIX}/metrics/feature_importance.parquet",
        ROC_CURVE_PATH: f"{ML_OBJECT_PREFIX}/metrics/roc_curve.parquet",
    }

    for local_path, object_key in artifacts.items():
        s3.upload_file(
            local_path,
            RUSTFS_BUCKET,
            object_key,
        )

        response = s3.head_object(
            Bucket=RUSTFS_BUCKET,
            Key=object_key,
        )

        size = response["ContentLength"]

        if size == 0:
            raise ValueError(f"Uploaded object is empty: {object_key}")

        print(
            f"Uploaded s3://{RUSTFS_BUCKET}/{object_key} "
            f"({size:,} bytes)"
        )


with DAG(
    dag_id="lakehouse_4_ml",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
) as dag:

    load_features = PythonOperator(
        task_id="1_load_feature_store",
        python_callable=load_feature_store,
    )

    train = PythonOperator(
        task_id="2_train_xgboost_model",
        python_callable=train_model,
    )

    evaluate = PythonOperator(
        task_id="3_evaluate_model",
        python_callable=evaluate_model,
    )

    write_artifacts = PythonOperator(
        task_id="4_write_artifacts_to_rustfs",
        python_callable=write_artifacts_to_rustfs,
    )

    load_features >> train >> evaluate >> write_artifacts
