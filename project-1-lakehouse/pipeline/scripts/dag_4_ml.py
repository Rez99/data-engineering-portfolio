from datetime import datetime
import os
import pickle

import duckdb
import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier


POLARIS_URI = "http://host.docker.internal:8181/api/catalog"

FEATURE_STORE_PATH = "/tmp/mart_session.parquet"
MODEL_PATH = "/tmp/xgboost_model.pkl"


def load_feature_store():
    """Extract feature store from Polaris and cache locally."""

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


def train_model():
    """Train XGBoost model and save artifacts."""

    df = pd.read_parquet(FEATURE_STORE_PATH)

    df["brand"] = df["brand"].astype("category")
    df["category_code"] = df["category_code"].astype("category")

    X = df.drop(columns=["converted"])
    y = df["converted"]

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

    with open(MODEL_PATH, "wb") as f:
        pickle.dump(
            {
                "model": model,
                "X_test": X_test,
                "y_test": y_test,
            },
            f,
        )


def evaluate_model():
    """Load model and print evaluation metrics."""

    with open(MODEL_PATH, "rb") as f:
        artifacts = pickle.load(f)

    model = artifacts["model"]
    X_test = artifacts["X_test"]
    y_test = artifacts["y_test"]

    y_prob = model.predict_proba(X_test)[:, 1]
    y_pred = model.predict(X_test)

    print("\nROC AUC")
    print("-------")
    print(f"{roc_auc_score(y_test, y_prob):.4f}")

    print("\nClassification Report")
    print("---------------------")
    print(classification_report(y_test, y_pred))

    print("\nConfusion Matrix")
    print("----------------")
    print(confusion_matrix(y_test, y_pred))

    importance = (
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
    print(importance.to_string(index=False))


with DAG(
    dag_id="lakehouse_4_ml",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
) as dag:

    load_features = PythonOperator(
        task_id="load_feature_store",
        python_callable=load_feature_store,
    )

    train = PythonOperator(
        task_id="train_xgboost_model",
        python_callable=train_model,
    )

    evaluate = PythonOperator(
        task_id="evaluate_model",
        python_callable=evaluate_model,
    )

    load_features >> train >> evaluate