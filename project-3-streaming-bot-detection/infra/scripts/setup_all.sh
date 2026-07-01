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

cd "${PROJECT_DIR}"

docker compose "${COMPOSE_FILES[@]}" up -d \
  redpanda redpanda-console jobmanager taskmanager postgres grafana

docker compose "${COMPOSE_FILES[@]}" exec -T postgres \
  psql -U clickstream -d clickstream -f /dev/stdin < "${PROJECT_DIR}/sql/postgres_schema.sql"

if ! docker compose "${COMPOSE_FILES[@]}" exec -T jobmanager /opt/flink/bin/flink list -r 2>/dev/null \
  | grep -q 'm2-clickstream-validation'; then
  docker compose "${COMPOSE_FILES[@]}" up -d --force-recreate validation-job
fi

if [[ -f "${PROJECT_DIR}/data/flink/generated/flink_job_operational.sql.template" ]] \
  && [[ -f "${PROJECT_DIR}/batch/artifacts/bot_config.json" ]] \
  && ! docker compose "${COMPOSE_FILES[@]}" exec -T jobmanager /opt/flink/bin/flink list -r 2>/dev/null \
    | grep -q 'm4-operational-bot-scoring'; then
  docker compose "${COMPOSE_FILES[@]}" up -d --force-recreate operational-job
fi

if docker compose "${COMPOSE_FILES[@]}" exec -T jobmanager /opt/flink/bin/flink list -r 2>/dev/null \
  | grep -q 'm3-clean-clickstream-parquet'; then
  docker compose "${COMPOSE_FILES[@]}" exec -T jobmanager bash -lc \
    "/opt/flink/bin/flink list -r | awk '/m3-clean-clickstream-parquet/ {print \$4}' | xargs -r -n1 /opt/flink/bin/flink cancel" || true
fi

if ! docker compose "${COMPOSE_FILES[@]}" exec -T jobmanager /opt/flink/bin/flink list -r 2>/dev/null \
  | grep -q 'm6-analytics-observer'; then
  docker compose "${COMPOSE_FILES[@]}" up -d --force-recreate analytics-observer-job
fi

cat <<REPORT
Complete streaming platform is available.

Redpanda Console: http://localhost:8080
Flink Web UI: http://localhost:8081
Grafana: http://localhost:3000

Run ./infra/scripts/verify_all.sh to verify the platform.
REPORT
