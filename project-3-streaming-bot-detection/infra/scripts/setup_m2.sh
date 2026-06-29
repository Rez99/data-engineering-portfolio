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
)

create_topic_if_missing() {
  local topic="$1"
  local partitions="$2"
  local retention_ms="$3"

  if docker compose "${COMPOSE_FILES[@]}" exec -T redpanda \
    rpk topic describe "${topic}" --brokers localhost:9092 >/dev/null 2>&1; then
    echo "Topic already exists: ${topic}"
    return
  fi

  docker compose "${COMPOSE_FILES[@]}" exec -T redpanda \
    rpk topic create "${topic}" \
      --brokers localhost:9092 \
      --partitions "${partitions}" \
      --replicas 1 \
      --topic-config "retention.ms=${retention_ms}" \
      --topic-config "cleanup.policy=delete"
}

mkdir -p "${CONNECTOR_DIR}"
if [[ ! -f "${KAFKA_CONNECTOR_JAR}" ]]; then
  echo "Downloading Flink Kafka SQL connector..."
  curl -fL "${KAFKA_CONNECTOR_URL}" -o "${KAFKA_CONNECTOR_JAR}"
fi

docker compose "${COMPOSE_FILES[@]}" up -d --remove-orphans redpanda redpanda-console jobmanager taskmanager

echo "Creating M1/M2 topics..."
create_topic_if_missing clickstream-raw 3 604800000
create_topic_if_missing clickstream-clean 3 604800000
create_topic_if_missing clickstream-dlq 1 1209600000

docker compose "${COMPOSE_FILES[@]}" up -d --force-recreate validation-job

echo
echo "M2 validation layer is starting."
echo "Flink UI: http://localhost:8081"
echo "Redpanda Console: http://localhost:8080"
echo
echo "Validation job logs:"
docker compose "${COMPOSE_FILES[@]}" logs --tail 40 validation-job
