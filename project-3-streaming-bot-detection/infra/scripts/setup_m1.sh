#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
COMPOSE_FILE="${PROJECT_DIR}/infra/compose/kafka.yml"

create_topic_if_missing() {
  local topic="$1"
  local partitions="$2"
  local retention_ms="$3"

  if docker compose -f "${COMPOSE_FILE}" exec -T redpanda \
    rpk topic describe "${topic}" --brokers localhost:9092 >/dev/null 2>&1; then
    echo "Topic already exists: ${topic}"
    return
  fi

  docker compose -f "${COMPOSE_FILE}" exec -T redpanda \
    rpk topic create "${topic}" \
      --brokers localhost:9092 \
      --partitions "${partitions}" \
      --replicas 1 \
      --topic-config "retention.ms=${retention_ms}" \
      --topic-config "cleanup.policy=delete"
}

docker compose -f "${COMPOSE_FILE}" up -d --remove-orphans redpanda

echo "Creating M1 topics..."
create_topic_if_missing clickstream-raw 3 604800000
create_topic_if_missing clickstream-clean 3 604800000
create_topic_if_missing clickstream-dlq 1 1209600000

echo
echo "M1 broker is ready."
echo "Kafka bootstrap server: localhost:19092"
echo "Topics:"
docker compose -f "${COMPOSE_FILE}" exec -T redpanda rpk topic list --brokers localhost:9092
