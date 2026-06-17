# dbt Transformation Project

This project migrates the Project 1 DuckDB models to Spark SQL:

```text
polaris.bronze.events
        |
        v
polaris.bronze.stg_events
        |
        v
polaris.silver.int_sessions
        |
        v
polaris.gold.features
```

The checked-in Thrift profile contains placeholder connection values and is
used only for static parsing. At runtime, `deployment/spark/run_dbt.py` writes
a session profile that configures `polaris` as Spark's active Iceberg catalog.

Run the M3.1 check from the repository root:

```bash
docker run --rm \
  -v "$PWD/deployment/dbt:/app" \
  -w /app \
  python:3.12-slim \
  sh -c \
  'pip install --quiet --no-cache-dir -r requirements.txt && dbt parse --profiles-dir .'
```
