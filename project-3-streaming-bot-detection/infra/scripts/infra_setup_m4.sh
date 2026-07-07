#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ARTIFACT_DIR="${PROJECT_DIR}/batch/artifacts"
SOURCE_CSV="${PROJECT_DIR}/data/source/2019-Oct.csv.gz"
SOURCE_CSV_DOCKER="/work/data/source/2019-Oct.csv.gz"
NORMALIZATION_SQL_PATH="${PROJECT_DIR}/batch/normalization.sql"
GENERATED_FLINK_DIR="${PROJECT_DIR}/data/flink/generated"
NORMALIZATION_VALUES_CSV="${GENERATED_FLINK_DIR}/normalization_values.csv"
CONNECTOR_DIR="${PROJECT_DIR}/data/flink/lib"
JAVA_DIR="${PROJECT_DIR}/streaming/java"
OPERATIONAL_JAR="${GENERATED_FLINK_DIR}/operational-bot-scoring.jar"
MAVEN_IMAGE="${MAVEN_IMAGE:-maven:3.9.9-eclipse-temurin-17}"
KAFKA_CONNECTOR_JAR="${CONNECTOR_DIR}/flink-sql-connector-kafka-3.2.0-1.19.jar"
KAFKA_CONNECTOR_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar"
PARQUET_CONNECTOR_JAR="${CONNECTOR_DIR}/flink-sql-parquet-1.19.1.jar"
PARQUET_CONNECTOR_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-sql-parquet/1.19.1/flink-sql-parquet-1.19.1.jar"
HADOOP_RUNTIME_JAR="${CONNECTOR_DIR}/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar"
HADOOP_RUNTIME_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar"
JDBC_CONNECTOR_JAR="${CONNECTOR_DIR}/flink-connector-jdbc-3.2.0-1.19.jar"
JDBC_CONNECTOR_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar"
POSTGRES_DRIVER_JAR="${CONNECTOR_DIR}/postgresql-42.7.3.jar"
POSTGRES_DRIVER_URL="https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar"
DUCKDB_COMPOSE_FILES=(-f "${PROJECT_DIR}/infra/compose/duckdb.yml")
STREAMING_COMPOSE_FILES=(
  -f "${PROJECT_DIR}/infra/compose/kafka.yml"
  -f "${PROJECT_DIR}/infra/compose/kafka-ui.yml"
  -f "${PROJECT_DIR}/infra/compose/flink.yml"
  -f "${PROJECT_DIR}/infra/compose/postgres.yml"
)
EVENTS_VIEW_SQL="CREATE OR REPLACE TEMP VIEW events AS SELECT * FROM read_csv_auto('${SOURCE_CSV_DOCKER}');"

cd "${PROJECT_DIR}"

download_if_missing() {
  local url="$1"
  local destination="$2"

  if [[ ! -f "${destination}" ]]; then
    echo "Downloading $(basename "${destination}")..."
    curl -fL "${url}" -o "${destination}"
  fi
}

require_topic() {
  local topic="$1"

  if ! docker compose "${STREAMING_COMPOSE_FILES[@]}" exec -T redpanda \
    rpk topic describe "${topic}" --brokers localhost:9092 >/dev/null 2>&1; then
    echo "M4 setup failed: Kafka topic is missing: ${topic}" >&2
    echo "Run ./infra/scripts/infra_setup_m1.sh first." >&2
    exit 1
  fi
}

wait_for_flink_job() {
  local job_name="$1"
  local setup_hint="$2"

  for _ in {1..30}; do
    if docker compose "${STREAMING_COMPOSE_FILES[@]}" exec -T jobmanager \
      /opt/flink/bin/flink list -r 2>/dev/null | grep -q "${job_name}"; then
      return
    fi
    sleep 2
  done

  echo "M4 setup failed: required Flink job is not running: ${job_name}" >&2
  echo "${setup_hint}" >&2
  exit 1
}

flink_job_running() {
  local job_name="$1"

  docker compose "${STREAMING_COMPOSE_FILES[@]}" exec -T jobmanager \
    /opt/flink/bin/flink list -r 2>/dev/null | grep -q "${job_name}"
}

build_operational_job() {
  mkdir -p "${GENERATED_FLINK_DIR}"

  docker run --rm \
    -v "${JAVA_DIR}:/work" \
    -v "${GENERATED_FLINK_DIR}/m2:/root/.m2" \
    -w /work \
    "${MAVEN_IMAGE}" \
    mvn -q -DskipTests package

  cp "${JAVA_DIR}/target/operational-bot-scoring-1.0.0.jar" \
    "${OPERATIONAL_JAR}"

  cat <<REPORT
Operational Flink job built:
  data/flink/generated/operational-bot-scoring.jar
REPORT
}

require_topic clickstream-clean

if [[ ! -f "${SOURCE_CSV}" ]]; then
  echo "M4 setup failed: source dataset not found at ${SOURCE_CSV}" >&2
  exit 1
fi

mkdir -p "${ARTIFACT_DIR}"

duckdb_scalar() {
  local sql="$1"
  docker compose "${DUCKDB_COMPOSE_FILES[@]}" run --rm -T duckdb \
    -csv -noheader -c "${EVENTS_VIEW_SQL} ${sql}" | tr -d '\r'
}

duckdb_csv() {
  local sql="$1"
  docker compose "${DUCKDB_COMPOSE_FILES[@]}" run --rm -T duckdb \
    -csv -noheader -c "${EVENTS_VIEW_SQL} ${sql}" | tr -d '\r'
}

duckdb_exec() {
  local sql="$1"
  docker compose "${DUCKDB_COMPOSE_FILES[@]}" run --rm -T duckdb \
    -c "${EVENTS_VIEW_SQL} ${sql}"
}

write_normalization_values_csv() {
  mkdir -p "${GENERATED_FLINK_DIR}"
  docker compose "${DUCKDB_COMPOSE_FILES[@]}" run --rm -T duckdb \
    -c "
      COPY (
      SELECT
        percentile,
        percentile_fraction,
        mean_click_interval_ms,
        min_click_interval_ms,
        max_click_interval_ms,
        sd_click_interval_ms
      FROM read_parquet('/work/batch/artifacts/normalization.parquet')
      ORDER BY percentile
      )
      TO '/work/data/flink/generated/normalization_values.csv'
      (HEADER, DELIMITER ',');
    "
}

if [[ -f "${ARTIFACT_DIR}/normalization.parquet" && -f "${ARTIFACT_DIR}/bot_config.json" ]]; then
  echo "M4 reference artifacts already exist; reusing batch/artifacts."
  echo "Run ./infra/scripts/infra_reset_m4.sh first if you need to regenerate them."
else
source_rows="$(duckdb_scalar "SELECT COUNT(*) FROM events;")"
schema_csv="$(duckdb_csv "DESCRIBE SELECT * FROM events;")"
required_schema_columns=0
for required_column in event_time event_type product_id user_id user_session; do
  if printf '%s\n' "${schema_csv}" | awk -F, -v column="${required_column}" '$1 == column { found = 1 } END { exit !found }'; then
    required_schema_columns=$((required_schema_columns + 1))
  fi
done

if [[ "${source_rows}" -eq 0 ]]; then
  echo "M4 setup failed: DuckDB queried the M3 dataset but found zero rows." >&2
  exit 1
fi

if [[ "${required_schema_columns}" -ne 5 ]]; then
  echo "M4 setup failed: M3 dataset schema is missing one or more required columns." >&2
  exit 1
fi

normalization_query="$(<"${NORMALIZATION_SQL_PATH}")"
duckdb_exec "
  COPY (${normalization_query})
  TO '/work/batch/artifacts/normalization.parquet'
  (FORMAT PARQUET);
"

session_count="$(duckdb_scalar "
  WITH ordered_events AS (
    SELECT user_session, CAST(event_time AS TIMESTAMP) AS event_time
    FROM events
    WHERE user_session IS NOT NULL AND event_time IS NOT NULL
  ),
  click_intervals AS (
    SELECT
      user_session,
      EXTRACT(EPOCH FROM (
        event_time - LAG(event_time) OVER (
          PARTITION BY user_session
          ORDER BY event_time
        )
      )) * 1000 AS click_interval_ms
    FROM ordered_events
  ),
  session_metrics AS (
    SELECT user_session, MAX(click_interval_ms) AS max_click_interval_ms
    FROM click_intervals
    WHERE click_interval_ms IS NOT NULL AND click_interval_ms >= 0
    GROUP BY user_session
  )
  SELECT COUNT(*) FROM session_metrics;
")"

session_inactivity_timeout_ms="$(duckdb_scalar "
  WITH ordered_events AS (
    SELECT user_session, CAST(event_time AS TIMESTAMP) AS event_time
    FROM events
    WHERE user_session IS NOT NULL AND event_time IS NOT NULL
  ),
  click_intervals AS (
    SELECT
      user_session,
      EXTRACT(EPOCH FROM (
        event_time - LAG(event_time) OVER (
          PARTITION BY user_session
          ORDER BY event_time
        )
      )) * 1000 AS click_interval_ms
    FROM ordered_events
  ),
  session_metrics AS (
    SELECT user_session, MAX(click_interval_ms) AS max_click_interval_ms
    FROM click_intervals
    WHERE click_interval_ms IS NOT NULL AND click_interval_ms >= 0
    GROUP BY user_session
  )
  SELECT CAST(ceil(quantile_cont(max_click_interval_ms, 0.99)) AS BIGINT)
  FROM session_metrics;
")"

generated_at_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

cat > "${ARTIFACT_DIR}/bot_config.json" <<CONFIG
{
  "bot_score_threshold": 0.7,
  "generated_at_utc": "${generated_at_utc}",
  "normalization_artifact": "batch/artifacts/normalization.parquet",
  "session_count": ${session_count},
  "session_inactivity_timeout_ms": ${session_inactivity_timeout_ms},
  "session_inactivity_timeout_source": {
    "metric": "max_click_interval_ms",
    "percentile": 99
  },
  "source_dataset": "data/source/2019-Oct.csv.gz",
  "source_row_count": ${source_rows}
}
CONFIG
fi

mkdir -p "${CONNECTOR_DIR}" "${PROJECT_DIR}/data/flink/checkpoints/m4-operational"
write_normalization_values_csv
download_if_missing "${KAFKA_CONNECTOR_URL}" "${KAFKA_CONNECTOR_JAR}"
download_if_missing "${PARQUET_CONNECTOR_URL}" "${PARQUET_CONNECTOR_JAR}"
download_if_missing "${HADOOP_RUNTIME_URL}" "${HADOOP_RUNTIME_JAR}"
download_if_missing "${JDBC_CONNECTOR_URL}" "${JDBC_CONNECTOR_JAR}"
download_if_missing "${POSTGRES_DRIVER_URL}" "${POSTGRES_DRIVER_JAR}"
build_operational_job

docker compose "${STREAMING_COMPOSE_FILES[@]}" up -d \
  postgres

echo "Waiting for PostgreSQL..."
for _ in {1..30}; do
  if docker compose "${STREAMING_COMPOSE_FILES[@]}" exec -T postgres \
    pg_isready -U clickstream -d clickstream >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

if ! docker compose "${STREAMING_COMPOSE_FILES[@]}" exec -T postgres \
  pg_isready -U clickstream -d clickstream >/dev/null 2>&1; then
  echo "M4 setup failed: PostgreSQL did not become ready." >&2
  exit 1
fi

wait_for_flink_job m2-clickstream-validation "Run ./infra/scripts/infra_setup_m2.sh first."

docker compose "${STREAMING_COMPOSE_FILES[@]}" exec -T postgres \
  psql -U clickstream -d clickstream -f /dev/stdin < "${PROJECT_DIR}/sql/postgres_schema.sql"

if ! flink_job_running m4-operational-bot-scoring; then
  docker compose "${STREAMING_COMPOSE_FILES[@]}" up -d --force-recreate operational-job
fi
wait_for_flink_job m4-operational-bot-scoring "Check operational-job logs for submission errors."

echo
echo "M4 operational pipeline is starting."
echo "DuckDB is available through Docker Compose: infra/compose/duckdb.yml"
echo "Artifacts: batch/artifacts/normalization.parquet and batch/artifacts/bot_config.json"
echo "PostgreSQL: localhost:5432 database=clickstream user=clickstream password=clickstream"
echo "Flink UI: http://localhost:8081"
echo "Redpanda Console: http://localhost:8080"
echo
echo "Operational job logs:"
docker compose "${STREAMING_COMPOSE_FILES[@]}" logs --tail 80 operational-job
