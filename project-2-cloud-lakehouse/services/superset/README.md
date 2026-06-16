# Superset Service

This image ports the Project 1 Superset configuration to Cloud Run. Superset
stores its application metadata in a dedicated PostgreSQL database and user on
the existing Cloud SQL instance.

At startup, the service downloads the five model-evaluation Parquet artifacts
from GCS and exposes them as views in a local DuckDB database. The bootstrap job
imports the Project 1 Superset database, datasets, charts, and model-evaluation
dashboard from `assets/`.
