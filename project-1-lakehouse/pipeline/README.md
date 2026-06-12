# Lakehouse Pipeline

This local pipeline demonstrates an end-to-end lakehouse workflow:

1. Airflow extracts ecommerce events into RustFS.
2. DuckDB writes and queries Iceberg tables registered in Polaris.
3. dbt builds the analytical session model.
4. XGBoost trains from the transformed data and publishes evaluation metrics.
5. Superset reads the metrics through DuckDB and serves a model dashboard.

## Services

| Service | Purpose | URL |
| --- | --- | --- |
| Airflow | Pipeline orchestration | http://localhost:8080 |
| RustFS | S3-compatible object storage | http://localhost:9001 |
| Polaris | Iceberg catalog | http://localhost:8181 |
| Superset | Model evaluation dashboard | http://localhost:8088 |

The project keeps separate Compose files for each infrastructure concern:

```text
pipeline/
├── dags/                   # Airflow DAG definitions
├── docker/
│   ├── airflow/            # Airflow, Postgres, and Redis
│   ├── dbt/                # dbt project and DuckDB profile
│   ├── polaris/            # RustFS and Polaris
│   └── superset/           # Superset and its metadata database
├── logs/
│   └── setup.log           # Detailed installer output
└── scripts/
    ├── setup.sh            # Start infrastructure and run the pipeline
    ├── reset.sh            # Delete containers, volumes, and generated env files
    ├── superset_config.py   # Superset application configuration
    ├── superset_init_duckdb.py
    └── superset_assets/    # Version-controlled dashboard definitions
```

## Run

Requirements: Docker Desktop, `curl`, `jq`, and `openssl`.

```bash
./project-1-lakehouse/pipeline/scripts/setup.sh
```

Setup preserves existing volumes. It performs these steps:

1. Starts RustFS and Polaris.
2. Provisions the Polaris catalog and Airflow credentials.
3. Initializes Airflow.
4. Starts Airflow and waits for health checks and DAG discovery.
5. Starts Superset and waits for its health check.
6. Triggers the Airflow pipeline and waits for success.
7. Registers the generated ML metrics and imports the Superset dashboard.

The terminal shows only high-level installer progress. Detailed Docker,
Airflow, and Superset output is written to `pipeline/logs/setup.log` and shown
automatically if a setup step fails.

Local credentials:

- Airflow: `airflow` / `airflow`
- Superset: `admin` / `admin`

Model dashboard:

http://localhost:8088/superset/dashboard/xgboost-model-evaluation/

## Reset

Use the reset command when a completely clean environment is required:

```bash
./project-1-lakehouse/pipeline/scripts/reset.sh
```

This is intentionally separate from normal setup because it deletes local
containers, volumes, generated credentials, and Airflow runtime files.
