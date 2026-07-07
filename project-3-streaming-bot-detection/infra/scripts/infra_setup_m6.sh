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

flink_running_jobs() {
  docker compose "${COMPOSE_FILES[@]}" exec -T jobmanager \
    /opt/flink/bin/flink list -r 2>/dev/null || true
}

wait_for_flink_job() {
  local job_name="$1"
  local setup_hint="$2"
  local jobs

  for _ in {1..30}; do
    jobs="$(flink_running_jobs)"
    if grep -q "${job_name}" <<<"${jobs}"; then
      return
    fi
    sleep 2
  done

  echo "M6 setup failed: required Flink job is not running: ${job_name}" >&2
  echo "${setup_hint}" >&2
  exit 1
}

docker compose "${COMPOSE_FILES[@]}" up -d \
  redpanda-console grafana

wait_for_flink_job m2-clickstream-validation "Run ./infra/scripts/infra_setup_m2.sh first."
wait_for_flink_job m3-clean-clickstream-parquet "Run ./infra/scripts/infra_setup_m3.sh first."
wait_for_flink_job m4-operational-bot-scoring "Run ./infra/scripts/infra_setup_m4.sh first."

cat <<REPORT
M6 observability stack is starting.
Redpanda Console: http://localhost:8080
Flink Web UI: http://localhost:8081
Grafana: http://localhost:3000
REPORT
