import os

import duckdb


DATABASE_PATH = "/app/superset_home/ml_metrics.duckdb"
RUSTFS_BUCKET = "lakehouse-bucket"
METRICS_PREFIX = "ml/xgboost_conversion/metrics"


def sql_string(value):
    return value.replace("'", "''")


def main():
    connection = duckdb.connect(DATABASE_PATH)

    connection.execute(f"""
        CREATE OR REPLACE PERSISTENT SECRET rustfs (
            TYPE s3,
            KEY_ID '{sql_string(os.environ["RUSTFS_ACCESS_KEY"])}',
            SECRET '{sql_string(os.environ["RUSTFS_SECRET_KEY"])}',
            REGION 'us-west-2',
            ENDPOINT 'host.docker.internal:9000',
            URL_STYLE 'path',
            USE_SSL false
        )
    """)

    connection.execute("CREATE SCHEMA IF NOT EXISTS ml")

    for view_name in (
        "metrics",
        "confusion_matrix",
        "feature_importance",
        "roc_curve",
    ):
        object_path = (
            f"s3://{RUSTFS_BUCKET}/{METRICS_PREFIX}/{view_name}.parquet"
        )
        connection.execute(f"""
            CREATE OR REPLACE VIEW ml.{view_name} AS
            SELECT *
            FROM read_parquet('{object_path}')
        """)

    view_counts = connection.execute("""
        SELECT 'metrics' AS view_name, count(*) AS row_count FROM ml.metrics
        UNION ALL
        SELECT 'confusion_matrix', count(*) FROM ml.confusion_matrix
        UNION ALL
        SELECT 'feature_importance', count(*) FROM ml.feature_importance
        UNION ALL
        SELECT 'roc_curve', count(*) FROM ml.roc_curve
    """).fetchall()

    for view_name, row_count in view_counts:
        print(f"Validated ml.{view_name}: {row_count:,} rows")


if __name__ == "__main__":
    main()
