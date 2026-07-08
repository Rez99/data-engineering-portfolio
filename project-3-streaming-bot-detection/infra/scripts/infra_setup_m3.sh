#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
source "${SCRIPT_DIR}/lib/docker_diagnostics.sh"
CONNECTOR_DIR="${PROJECT_DIR}/data/flink/lib"
KAFKA_CONNECTOR_JAR="${CONNECTOR_DIR}/flink-sql-connector-kafka-3.2.0-1.19.jar"
KAFKA_CONNECTOR_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar"
PARQUET_CONNECTOR_JAR="${CONNECTOR_DIR}/flink-sql-parquet-1.19.1.jar"
PARQUET_CONNECTOR_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-sql-parquet/1.19.1/flink-sql-parquet-1.19.1.jar"
HADOOP_RUNTIME_JAR="${CONNECTOR_DIR}/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar"
HADOOP_RUNTIME_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar"
COMPOSE_FILES=(
  -f "${PROJECT_DIR}/infra/compose/kafka.yml"
  -f "${PROJECT_DIR}/infra/compose/kafka-ui.yml"
  -f "${PROJECT_DIR}/infra/compose/flink.yml"
  -f "${PROJECT_DIR}/infra/compose/postgres.yml"
)

require_topic() {
  local topic="$1"

  if ! docker_check_or_explain "M3 setup failed while checking Kafka topic ${topic}" \
    docker compose "${COMPOSE_FILES[@]}" exec -T redpanda \
      rpk topic describe "${topic}" --brokers localhost:9092; then
    echo "M3 setup failed: Kafka topic is missing: ${topic}" >&2
    echo "Run ./infra/scripts/infra_setup_m1.sh first." >&2
    exit 1
  fi
}

wait_for_flink_job() {
  local job_name="$1"
  local setup_hint="$2"

  for _ in {1..30}; do
    if docker_output_or_explain "M3 setup failed while waiting for Flink job ${job_name}" \
      docker compose "${COMPOSE_FILES[@]}" exec -T jobmanager \
        /opt/flink/bin/flink list -r | grep -q "${job_name}"; then
      return
    fi
    sleep 2
  done

  echo "M3 setup failed: required Flink job is not running: ${job_name}" >&2
  echo "${setup_hint}" >&2
  exit 1
}

flink_job_running() {
  local job_name="$1"

  docker_output_or_explain "M3 setup failed while listing Flink jobs" \
    docker compose "${COMPOSE_FILES[@]}" exec -T jobmanager \
      /opt/flink/bin/flink list -r | grep -q "${job_name}"
}

download_if_missing() {
  local url="$1"
  local destination="$2"

  if [[ ! -f "${destination}" ]]; then
    echo "Downloading $(basename "${destination}")..."
    curl -fL "${url}" -o "${destination}"
  fi
}

mkdir -p "${CONNECTOR_DIR}" "${PROJECT_DIR}/data/analytics"
require_topic clickstream-clean
download_if_missing "${KAFKA_CONNECTOR_URL}" "${KAFKA_CONNECTOR_JAR}"
download_if_missing "${PARQUET_CONNECTOR_URL}" "${PARQUET_CONNECTOR_JAR}"
download_if_missing "${HADOOP_RUNTIME_URL}" "${HADOOP_RUNTIME_JAR}"

docker compose "${COMPOSE_FILES[@]}" up -d jobmanager taskmanager
wait_for_flink_job m2-clickstream-validation "Run ./infra/scripts/infra_setup_m2.sh first."

if ! flink_job_running m3-clean-clickstream-parquet; then
  docker compose "${COMPOSE_FILES[@]}" up -d --force-recreate analytics-job
fi
wait_for_flink_job m3-clean-clickstream-parquet "Check analytics-job logs for submission errors."

echo
echo "M3 analytical pipeline is starting."
echo "Flink UI: http://localhost:8081"
echo "Redpanda Console: http://localhost:8080"
echo "Parquet output: data/analytics/clickstream"
echo
echo "Analytics job logs:"
docker compose "${COMPOSE_FILES[@]}" logs --tail 40 analytics-job
