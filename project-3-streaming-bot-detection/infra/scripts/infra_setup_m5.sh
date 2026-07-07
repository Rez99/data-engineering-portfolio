#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
COMPOSE_FILES=(
  -f "${PROJECT_DIR}/infra/compose/postgres.yml"
  -f "${PROJECT_DIR}/infra/compose/grafana.yml"
)
STREAMING_COMPOSE_FILES=(
  -f "${PROJECT_DIR}/infra/compose/kafka.yml"
  -f "${PROJECT_DIR}/infra/compose/kafka-ui.yml"
  -f "${PROJECT_DIR}/infra/compose/flink.yml"
  -f "${PROJECT_DIR}/infra/compose/postgres.yml"
)

cd "${PROJECT_DIR}"

docker compose "${COMPOSE_FILES[@]}" up -d grafana

tables_exist="$(docker compose "${COMPOSE_FILES[@]}" exec -T postgres \
  psql -U clickstream -d clickstream -At -c "SELECT to_regclass('public.session_bot_scores') IS NOT NULL AND to_regclass('public.stream_bot_metrics') IS NOT NULL;" | tr -d '\r')"

if [[ "${tables_exist}" != "t" ]]; then
  echo "M5 setup failed: PostgreSQL operational tables are missing." >&2
  echo "Run ./infra/scripts/infra_setup_m4.sh first." >&2
  exit 1
fi

session_rows="$(docker compose "${COMPOSE_FILES[@]}" exec -T postgres \
  psql -U clickstream -d clickstream -At -c "SELECT COUNT(*) FROM session_bot_scores;" | tr -d '\r')"
metric_rows="$(docker compose "${COMPOSE_FILES[@]}" exec -T postgres \
  psql -U clickstream -d clickstream -At -c "SELECT COUNT(*) FROM stream_bot_metrics;" | tr -d '\r')"

operational_job_status="not checked"
if docker compose "${STREAMING_COMPOSE_FILES[@]}" ps -q jobmanager >/dev/null 2>&1; then
  if docker compose "${STREAMING_COMPOSE_FILES[@]}" exec -T jobmanager /opt/flink/bin/flink list -r 2>/dev/null \
    | grep -q 'm4-operational-bot-scoring'; then
    operational_job_status="running"
  else
    operational_job_status="not running"
  fi
fi

cat <<REPORT
M5 Grafana dashboard is starting.
Grafana: http://localhost:3000
Login: admin / admin
Dashboard: Streaming / Streaming Bot Detection Live
PostgreSQL datasource: Clickstream Postgres
session_bot_scores rows: ${session_rows}
stream_bot_metrics rows: ${metric_rows}
M4 operational Flink job: ${operational_job_status}
REPORT
