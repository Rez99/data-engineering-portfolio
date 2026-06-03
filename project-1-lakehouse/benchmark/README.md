# Stop & start docker containers

```bash
# https://polaris.apache.org/releases/1.5.0/getting-started/quick-start/

cd ~/data-engineering-portfolio/project-1-lakehouse/benchmark

docker compose down -v
curl -s https://raw.githubusercontent.com/apache/polaris/refs/heads/main/site/content/guides/quickstart/docker-compose.yml \
| docker compose -f - down -v

docker compose up -d --build
curl -s https://raw.githubusercontent.com/apache/polaris/refs/heads/main/site/content/guides/quickstart/docker-compose.yml \
| docker compose -f - up -d

docker ps
```

# Download data
```bash
docker compose exec python python 0_download_dataset.py
docker compose exec python bash
```
```bash
ls /tmp/data
exit
```

# Pandas benchmark (with csv.gz)
```bash
docker stats
docker compose exec python python 1_benchmark_pandas.py
#CSV.gz size: 1.62 GB
#Loading CSV into Pandas...
#Load complete in 69.13 s
#Query results:
#event_type
#cart         10168
#purchase     16249
#view        268628
#Name: user_session, dtype: int64
#Query time: 0.21 s
```

# Postgres benchmark
```bash
docker compose exec python python 2_benchmark_postgres.py
#Data already loaded. Skipping load.
#Table size: 6850 MB
#Running benchmark query...
#Query results:
#('cart', 10168)
#('purchase', 16249)
#('view', 268628)
#Query time: 7.39 s
#Query time: 59.32 s
```

# DuckDB benchmark (with Parquet)
```bash
docker compose exec duckdb duckdb
```
```bash
COPY (
    SELECT *
    FROM read_csv_auto('/data/2019-Oct.csv.gz')
)
TO '/data/2019-Oct.parquet'
(FORMAT PARQUET);

.timer on

SELECT event_type, COUNT(DISTINCT user_session)
                               FROM read_parquet('/data/2019-Oct.parquet')
                               WHERE event_time::date = '2019-10-01'
                               GROUP BY event_type;

#┌────────────┬──────────────────────────────┐
#│ event_type │ count(DISTINCT user_session) │
#│  varchar   │            int64             │
#├────────────┼──────────────────────────────┤
#│ purchase   │                        16249 │
#│ cart       │                        10168 │
#│ view       │                       268628 │
#└────────────┴──────────────────────────────┘
#Run Time (s): real 0.126 user 0.392395 sys 0.244430

EXPLAIN ANALYZE
         SELECT
             event_type,
             COUNT(DISTINCT user_session)
         FROM read_parquet('/data/2019-Oct.parquet')
         WHERE event_time::DATE = DATE '2019-10-01'
         GROUP BY event_type;

SELECT
             row_group_id,
             path_in_schema,
             stats_min,
             stats_max
         FROM parquet_metadata('/data/2019-Oct.parquet')
         WHERE path_in_schema = 'event_time'
         LIMIT 20;
```
```bash
docker compose exec python bash
```
```bash                                                                                                                                           
ls -lh /data/2019-Oct.parquet
#-rw-r--r-- 1 root root 1.5G Jun  1 21:59 /data/2019-Oct.parquet
```

# DuckDB benchmark (with Iceberg + Polaris)
```bash
LOGS=$(docker logs benchmark-polaris-setup-1 2>&1)

CLIENT_ID=$(echo "$LOGS" | sed -nE "s/.*client_id=([^']+)'.*/\1/p" | head -1)
CLIENT_SECRET=$(echo "$LOGS" | sed -nE "s/.*client_secret=([^']+)'.*/\1/p" | head -1)

echo "$CLIENT_ID"
echo "$CLIENT_SECRET"

export TOKEN=$(
  curl -s -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
    -d "grant_type=client_credentials" \
    -d "client_id=$CLIENT_ID" \
    -d "client_secret=$CLIENT_SECRET" \
    -d "scope=PRINCIPAL_ROLE:ALL" \
  | jq -r '.access_token'
)

curl -X POST http://localhost:8181/api/catalog/v1/quickstart_catalog/namespaces \
-H "Authorization: Bearer $TOKEN" \
-H 'Content-Type: application/json' \
-d '{"namespace": ["my_namespace"], "properties": {}}'

curl -X GET http://localhost:8181/api/catalog/v1/quickstart_catalog/namespaces \
-H "Authorization: Bearer $TOKEN"


docker logs benchmark-polaris-setup-1 | awk '
/Client ID:/ {id=$3}
/Client Secret:/ {secret=$3}
END {
  print "ATTACH '\''quickstart_catalog'\'' AS polaris ("
  print "    TYPE iceberg,"
  print "    CLIENT_ID '\''" id "'\'',"
  print "    CLIENT_SECRET '\''" secret "'\'',"
  print "    ENDPOINT '\''http://localhost:8181/api/catalog'\'',"
  print "    ACCESS_DELEGATION_MODE '\''vended_credentials'\''"
  print ");"
}'

docker compose exec duckdb duckdb

ATTACH ...

USE polaris.my_namespace;

CREATE TABLE polaris.my_namespace.events_iceberg AS
SELECT * FROM
read_parquet('/data/2019-Oct.parquet');

.timer on
SELECT event_type, COUNT(DISTINCT user_session)
                               FROM polaris.my_namespace.events_iceberg
                               WHERE event_time::date = '2019-10-01'
                               GROUP BY event_type;
# Run Time (s): real 0.133 user 0.257458 sys 0.088026





SELECT *
FROM iceberg_metadata(polaris.my_namespace.events_iceberg)
LIMIT 20;

SELECT *
FROM iceberg_snapshots(polaris.my_namespace.events_iceberg);

EXPLAIN ANALYZE
                       SELECT event_type, COUNT(DISTINCT user_session)
                               FROM polaris.my_namespace.events_iceberg
                               WHERE event_time::date = '2019-10-01'
                               GROUP BY event_type;


```