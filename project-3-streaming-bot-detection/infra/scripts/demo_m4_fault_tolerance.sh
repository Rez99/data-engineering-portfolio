#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
COMPOSE_FILES=(
  -f "${PROJECT_DIR}/infra/compose/kafka.yml"
  -f "${PROJECT_DIR}/infra/compose/kafka-ui.yml"
  -f "${PROJECT_DIR}/infra/compose/flink.yml"
  -f "${PROJECT_DIR}/infra/compose/postgres.yml"
)

cd "${PROJECT_DIR}"

postgres_scalar() {
  local sql="$1"
  docker compose "${COMPOSE_FILES[@]}" exec -T postgres \
    psql -U clickstream -d clickstream -At -c "${sql}" | tr -d '\r'
}

flink_summary() {
  python3 - <<'PY'
import json
import sys
from urllib.request import urlopen

jobs = json.load(urlopen("http://localhost:8081/jobs/overview", timeout=5))["jobs"]
running = [job for job in jobs if job["name"] == "m4-operational-bot-scoring" and job["state"] == "RUNNING"]
if not running:
    print("missing|0|0")
    sys.exit(0)

job_id = running[0]["jid"]
checkpoints = json.load(urlopen(f"http://localhost:8081/jobs/{job_id}/checkpoints", timeout=5))
counts = checkpoints.get("counts", {})
print(f"{job_id}|{counts.get('completed', 0)}|{counts.get('restored', 0)}")
PY
}

wait_for_completed_checkpoint() {
  local label="$1"
  local expected_min="$2"
  local summary
  local job_id
  local completed
  local restored

  for _ in {1..60}; do
    summary="$(flink_summary)"
    IFS='|' read -r job_id completed restored <<<"${summary}"
    if [[ "${job_id}" != "missing" && "${completed}" -ge "${expected_min}" ]]; then
      echo "${label}: job_id=${job_id} completed_checkpoints=${completed} restored_checkpoints=${restored}"
      return 0
    fi
    sleep 5
  done

  echo "Timed out waiting for ${label} completed checkpoints >= ${expected_min}." >&2
  return 1
}

before_sessions="$(postgres_scalar "SELECT COUNT(*) FROM session_bot_scores;")"
before_metrics="$(postgres_scalar "SELECT COUNT(*) FROM stream_bot_metrics;")"
before_summary="$(flink_summary)"
IFS='|' read -r before_job_id before_completed before_restored <<<"${before_summary}"

if [[ "${before_job_id}" == "missing" ]]; then
  echo "M4 fault-tolerance demo failed: operational Flink job is not running." >&2
  exit 1
fi

wait_for_completed_checkpoint "before failure" 1

echo "Restarting TaskManager to trigger Flink task recovery from checkpoint..."
docker compose "${COMPOSE_FILES[@]}" restart taskmanager >/dev/null

wait_for_completed_checkpoint "after recovery" 1

after_sessions="$(postgres_scalar "SELECT COUNT(*) FROM session_bot_scores;")"
after_metrics="$(postgres_scalar "SELECT COUNT(*) FROM stream_bot_metrics;")"
after_summary="$(flink_summary)"
IFS='|' read -r after_job_id after_completed after_restored <<<"${after_summary}"

cat <<REPORT
M4 fault-tolerance demo
before_job_id=${before_job_id}
after_job_id=${after_job_id}
before_completed_checkpoints=${before_completed}
after_completed_checkpoints=${after_completed}
before_restored_checkpoints=${before_restored}
after_restored_checkpoints=${after_restored}
before_session_rows=${before_sessions}
after_session_rows=${after_sessions}
before_metric_rows=${before_metrics}
after_metric_rows=${after_metrics}
delivery_semantics=Flink checkpoints capture Kafka source offsets and operator state; PostgreSQL writes are idempotent UPSERTs, so replayed updates converge on the latest value for each primary key.
REPORT
