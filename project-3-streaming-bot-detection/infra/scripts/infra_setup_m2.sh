#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CONNECTOR_DIR="${PROJECT_DIR}/data/flink/lib"
KAFKA_CONNECTOR_JAR="${CONNECTOR_DIR}/flink-sql-connector-kafka-3.2.0-1.19.jar"
KAFKA_CONNECTOR_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar"
COMPOSE_FILES=(
  -f "${PROJECT_DIR}/infra/compose/kafka.yml"
  -f "${PROJECT_DIR}/infra/compose/kafka-ui.yml"
  -f "${PROJECT_DIR}/infra/compose/flink.yml"
  -f "${PROJECT_DIR}/infra/compose/postgres.yml"
)

require_topic() {
  local topic="$1"

  if ! docker compose "${COMPOSE_FILES[@]}" exec -T redpanda \
    rpk topic describe "${topic}" --brokers localhost:9092 >/dev/null 2>&1; then
    echo "M2 setup failed: Kafka topic is missing: ${topic}" >&2
    echo "Run ./infra/scripts/infra_setup_m1.sh first." >&2
    exit 1
  fi
}

cancel_flink_job_by_name() {
  local job_name="$1"

  docker compose "${COMPOSE_FILES[@]}" exec -T jobmanager bash -lc \
    "/opt/flink/bin/flink list -r | awk -v job='${job_name}' '\$0 ~ job {print \$4}' | xargs -r -n1 /opt/flink/bin/flink cancel" || true
}

wait_for_flink_job() {
  local job_name="$1"

  for _ in {1..30}; do
    if docker compose "${COMPOSE_FILES[@]}" exec -T jobmanager \
      /opt/flink/bin/flink list -r 2>/dev/null | grep -q "${job_name}"; then
      return
    fi
    sleep 2
  done

  echo "M2 setup failed: timed out waiting for Flink job: ${job_name}" >&2
  exit 1
}

require_topic clickstream-raw
require_topic clickstream-clean
require_topic clickstream-dlq

mkdir -p "${CONNECTOR_DIR}"
if [[ ! -f "${KAFKA_CONNECTOR_JAR}" ]]; then
  echo "Downloading Flink Kafka SQL connector..."
  curl -fL "${KAFKA_CONNECTOR_URL}" -o "${KAFKA_CONNECTOR_JAR}"
fi

docker compose "${COMPOSE_FILES[@]}" up -d --remove-orphans jobmanager taskmanager

cancel_flink_job_by_name m2-clickstream-validation
docker compose "${COMPOSE_FILES[@]}" up -d --force-recreate validation-job
wait_for_flink_job m2-clickstream-validation

echo
echo "M2 validation layer is starting."
echo "Flink UI: http://localhost:8081"
echo "Redpanda Console: http://localhost:8080"
echo
echo "Validation job logs:"
docker compose "${COMPOSE_FILES[@]}" logs --tail 40 validation-job
