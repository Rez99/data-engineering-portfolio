import os

import duckdb
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    roc_auc_score,
    classification_report,
    confusion_matrix,
)
from xgboost import XGBClassifier

# python3 ../docker/docker-airflow/ml/train_xgboost_conversion.py

# ------------------------------------------------------------------
# Connect to Polaris
# ------------------------------------------------------------------

POLARIS_URI = "http://host.docker.internal:8181/api/catalog"
POLARIS_CLIENT_ID = os.environ["AIRFLOW_CLIENT_ID"]
POLARIS_CLIENT_SECRET = os.environ["AIRFLOW_CLIENT_SECRET"]

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
        CLIENT_ID '{POLARIS_CLIENT_ID}',
        CLIENT_SECRET '{POLARIS_CLIENT_SECRET}'
    );
""")


# ------------------------------------------------------------------
# Load feature store
# ------------------------------------------------------------------

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


# ------------------------------------------------------------------
# Prepare train/test sets
# ------------------------------------------------------------------

X = df.drop(columns=["converted"])
y = df["converted"]

X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
    random_state=42,
    stratify=y,
)


# ------------------------------------------------------------------
# Train XGBoost
# ------------------------------------------------------------------

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


# ------------------------------------------------------------------
# Evaluate
# ------------------------------------------------------------------

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


# ------------------------------------------------------------------
# Feature importance
# ------------------------------------------------------------------

importance = (
    pd.DataFrame(
        {
            "feature": X.columns,
            "importance": model.feature_importances_,
        }
    )
    .sort_values("importance", ascending=False)
)

print("\nFeature Importance")
print("------------------")
print(importance.to_string(index=False))