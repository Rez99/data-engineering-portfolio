#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
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

download_if_missing() {
  local url="$1"
  local destination="$2"

  if [[ ! -f "${destination}" ]]; then
    echo "Downloading $(basename "${destination}")..."
    curl -fL "${url}" -o "${destination}"
  fi
}

mkdir -p "${CONNECTOR_DIR}" "${PROJECT_DIR}/data/analytics"
download_if_missing "${KAFKA_CONNECTOR_URL}" "${KAFKA_CONNECTOR_JAR}"
download_if_missing "${PARQUET_CONNECTOR_URL}" "${PARQUET_CONNECTOR_JAR}"
download_if_missing "${HADOOP_RUNTIME_URL}" "${HADOOP_RUNTIME_JAR}"

docker compose "${COMPOSE_FILES[@]}" up -d --remove-orphans redpanda redpanda-console jobmanager taskmanager

echo "Creating M1/M2/M3 topics..."
create_topic_if_missing clickstream-raw 3 604800000
create_topic_if_missing clickstream-clean 3 604800000
create_topic_if_missing clickstream-dlq 1 1209600000

docker compose "${COMPOSE_FILES[@]}" exec -T jobmanager bash -lc \
  "/opt/flink/bin/flink list -r | awk '/ : / {print \$4}' | xargs -r -n1 /opt/flink/bin/flink cancel"
docker compose "${COMPOSE_FILES[@]}" up -d --force-recreate validation-job analytics-job

echo
echo "M3 analytical pipeline is starting."
echo "Flink UI: http://localhost:8081"
echo "Redpanda Console: http://localhost:8080"
echo "Parquet output: data/analytics/clickstream"
echo
echo "Analytics job logs:"
docker compose "${COMPOSE_FILES[@]}" logs --tail 40 analytics-job
