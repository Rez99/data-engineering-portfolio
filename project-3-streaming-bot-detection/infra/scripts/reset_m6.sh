#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
COMPOSE_FILES=(
  -f "${PROJECT_DIR}/infra/compose/kafka.yml"
  -f "${PROJECT_DIR}/infra/compose/kafka-ui.yml"
  -f "${PROJECT_DIR}/infra/compose/flink.yml"
  -f "${PROJECT_DIR}/infra/compose/postgres.yml"
  -f "${PROJECT_DIR}/infra/compose/grafana.yml"
)

docker compose "${COMPOSE_FILES[@]}" stop redpanda-console grafana || true
docker compose "${COMPOSE_FILES[@]}" rm -f -v redpanda-console grafana || true
docker volume rm project-3-streaming-bot-detection-m1_grafana-data >/dev/null 2>&1 || true

echo "Removed M6 monitoring UI containers. Core Kafka/Flink/PostgreSQL state was preserved."
