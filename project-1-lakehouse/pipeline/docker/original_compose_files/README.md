# Original docker compose files
## Apache Airflow
- Reference URL: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
- Docker compose file at above URL: https://airflow.apache.org/docs/apache-airflow/3.2.2/docker-compose.yaml
- Download date: Jun 4 2026
- Version: 3.2.2

```bash
echo '====== Download docker compose file yaml ======'
curl -L https://airflow.apache.org/docs/apache-airflow/3.2.2/docker-compose.yaml -o airflow-compose-original.yaml
```

## Apache Polaris
- Reference URL: https://polaris.apache.org/guides/quickstart/
- Docker compose file at above URL: https://raw.githubusercontent.com/apache/polaris/refs/heads/main/site/content/guides/quickstart/docker-compose.yml
- Download date: Jun 4 2026
- Version 1.5.0

```bash
echo '====== Download docker compose file yaml ======'
curl -L https://raw.githubusercontent.com/apache/polaris/apache-polaris-1.5.0/site/content/guides/quickstart/docker-compose.yml -o polaris-compose-original.yaml
```
# Clear tables form polaris and s3
```bash
docker exec docker-airflow-airflow-worker-1 bash -c "cd /opt/airflow/lakehouse && dbt show --inline 'select count(*) from polaris.gold.mart_session'"
```

```bash
docker exec -it docker-airflow-airflow-worker-1 python3

import os
import duckdb

con = duckdb.connect()

con.execute(f"""
ATTACH 'lakehouse' AS polaris (
    TYPE ICEBERG,
    ENDPOINT 'http://host.docker.internal:8181/api/catalog',
    CLIENT_ID '{os.environ["AIRFLOW_CLIENT_ID"]}',
    CLIENT_SECRET '{os.environ["AIRFLOW_CLIENT_SECRET"]}'
)
""")

print(con.execute("SHOW TABLES FROM polaris.bronze").fetchall())
print(con.execute("SHOW TABLES FROM polaris.silver").fetchall())
print(con.execute("SHOW TABLES FROM polaris.gold").fetchall())

con.execute('DROP TABLE IF EXISTS polaris.bronze."stg-2019-Oct"')
con.execute('DROP TABLE IF EXISTS polaris.silver."int_session"')
con.execute('DROP TABLE IF EXISTS polaris.gold."fact_session"')

print(con.execute("SHOW TABLES FROM polaris.bronze").fetchall())
print(con.execute("SHOW TABLES FROM polaris.silver").fetchall())
print(con.execute("SHOW TABLES FROM polaris.gold").fetchall())

```


```bash

docker exec docker-airflow-airflow-worker-1 bash -c "cd /opt/airflow/lakehouse && dbt run --select path:models/staging/stg-2019-Oct.sql"
docker exec docker-airflow-airflow-worker-1 bash -c "cd /opt/airflow/lakehouse && dbt run --select path:models/intermediate/int_session.sql"
docker exec docker-airflow-airflow-worker-1 bash -c "cd /opt/airflow/lakehouse && dbt run --select path:models/mart/fact_session.sql"
docker exec docker-airflow-airflow-worker-1 bash -c "cd /opt/airflow/lakehouse && dbt run --select +path:models/mart/fact_session.sql"


```

## Engineering Notes and Design Decisions

### 1. Load pipeline redesign: introducing an intermediate Parquet stage

The initial implementation loaded the Iceberg table directly from the compressed CSV:

```text
CSV.GZ → Iceberg
```

using a single statement:

```sql
CREATE TABLE iceberg_table AS
SELECT *
FROM read_csv_auto('raw-2019-Oct.csv.gz')
```

This coupled CSV decompression, schema inference, parsing, and Iceberg file generation into a single memory-intensive operation. During testing, the CSV parsing stage proved to be the primary resource bottleneck.

The pipeline was redesigned to introduce an intermediate Parquet artifact:

```text
CSV.GZ → Parquet → Iceberg
```

The revised implementation first converts the raw CSV to Parquet using DuckDB's `COPY ... TO (FORMAT parquet)` command, and only then creates the Iceberg table from the Parquet files. This separates the expensive row-oriented parsing workload from the Iceberg write, significantly reducing peak memory requirements while also mirroring common production lakehouse ingestion patterns.

---

### 2. dbt orchestration and memory management

The original transformation layer was orchestrated using a single dbt invocation:

```text
dbt run --select +mart_session
```

However, empirical testing revealed that while each model could be built successfully in isolation, chaining multiple large models together caused the Airflow worker to exceed its memory limit:

* `dbt run --select stg-2019-Oct` ✅
* `dbt run --select int_session` ✅
* `dbt run --select stg-2019-Oct int_session` ❌ (OOM)
* `dbt run --select +mart_session` ❌ (OOM)

The evidence suggests that memory allocated by the embedded DuckDB process is not fully returned to the operating system between model executions within a single dbt invocation. As a result, the second large model begins execution before the memory footprint of the first has been fully reclaimed.

The practical solution was to orchestrate each model as a separate Airflow task:

```text
stg_2019_oct → int_session → mart_session
```

with each task executing its own `dbt run --select ...` command. This allows the dbt process to terminate between stages, releasing memory and reducing peak resource consumption. An additional benefit is improved observability and retry granularity within the Airflow UI.

---

### 3. General engineering lesson: optimize for peak memory, not total work

Both of the issues encountered during development were ultimately solved by introducing intermediate persistence boundaries:

* **Load layer:** materialize an intermediate Parquet dataset before creating the Iceberg table.
* **Transform layer:** materialize each dbt model in a separate process rather than building the entire dependency chain in a single invocation.

In both cases, the total amount of computation remains essentially unchanged. The improvement comes from reducing **peak memory utilization** by breaking one large operation into smaller, independently executable stages. Rather than solving the problem by allocating more hardware, the pipeline architecture was adapted to fit the resource constraints of the execution environment.
