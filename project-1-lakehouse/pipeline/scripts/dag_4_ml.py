from datetime import datetime
import os
import pickle

import boto3
import duckdb
import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    roc_auc_score,
    roc_curve,
)
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier


POLARIS_URI = "http://host.docker.internal:8181/api/catalog"

RUSTFS_ENDPOINT = "http://host.docker.internal:9000"
RUSTFS_ACCESS_KEY = os.environ["RUSTFS_ACCESS_KEY"]
RUSTFS_SECRET_KEY = os.environ["RUSTFS_SECRET_KEY"]

RUSTFS_BUCKET = "lakehouse-bucket"

LOCAL_DIR = "/opt/airflow/data/ml"
FEATURE_STORE_PATH = f"{LOCAL_DIR}/mart_session.parquet"
MODEL_PATH = f"{LOCAL_DIR}/xgboost_model.json"
TEST_DATA_PATH = f"{LOCAL_DIR}/test_data.pkl"

METRICS_PATH = f"{LOCAL_DIR}/metrics.parquet"
CONFUSION_MATRIX_PATH = f"{LOCAL_DIR}/confusion_matrix.parquet"
FEATURE_IMPORTANCE_PATH = f"{LOCAL_DIR}/feature_importance.parquet"
ROC_CURVE_PATH = f"{LOCAL_DIR}/roc_curve.parquet"

ML_OBJECT_PREFIX = "ml/xgboost_conversion"


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=RUSTFS_ENDPOINT,
        aws_access_key_id=RUSTFS_ACCESS_KEY,
        aws_secret_access_key=RUSTFS_SECRET_KEY,
    )


def get_polaris_connection():
    con = duckdb.connect()

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

    query = """
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
    """

    df = con.execute(query).fetchdf()

    df["brand"] = df["brand"].astype("category")
    df["category_code"] = df["category_code"].astype("category")

    print(f"Loaded {len(df):,} sessions")
    print()
    print("Target distribution:")
    print(df["converted"].value_counts(normalize=True))

    df.to_parquet(FEATURE_STORE_PATH)
    print(f"Wrote local feature store: {FEATURE_STORE_PATH}")


def train_model():
    df = pd.read_parquet(FEATURE_STORE_PATH)

    df["brand"] = df["brand"].astype("category")
    df["category_code"] = df["category_code"].astype("category")

    X = df.drop(columns=["converted"])
    y = df["converted"].astype(bool)

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
        stratify=y,
    )

    model = XGBClassifier(
        objective="binary:logistic",
        n_estimators=300,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        eval_metric="logloss",
        enable_categorical=True,
    )

    model.fit(X_train, y_train)

    model.save_model(MODEL_PATH)

    with open(TEST_DATA_PATH, "wb") as f:
        pickle.dump(
            {
                "X_test": X_test,
                "y_test": y_test,
            },
            f,
        )

    print(f"Saved local model: {MODEL_PATH}")
    print(f"Saved local test data: {TEST_DATA_PATH}")


def evaluate_model():
    model = XGBClassifier(enable_categorical=True)
    model.load_model(MODEL_PATH)

    with open(TEST_DATA_PATH, "rb") as f:
        test_data = pickle.load(f)

    X_test = test_data["X_test"]
    y_test = test_data["y_test"]

    y_prob = model.predict_proba(X_test)[:, 1]
    y_pred = model.predict(X_test).astype(bool)

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

    feature_importance_df = (
        pd.DataFrame(
            {
                "feature": X_test.columns,
                "importance": model.feature_importances_,
            }
        )
        .sort_values("importance", ascending=False)
    )

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