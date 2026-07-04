#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PARQUET_DIR="${PROJECT_DIR}/data/analytics/clickstream"
ARTIFACT_DIR="${PROJECT_DIR}/batch/artifacts"
COMPOSE_FILES=(
  -f "${PROJECT_DIR}/infra/compose/kafka.yml"
  -f "${PROJECT_DIR}/infra/compose/kafka-ui.yml"
  -f "${PROJECT_DIR}/infra/compose/flink.yml"
  -f "${PROJECT_DIR}/infra/compose/postgres.yml"
  -f "${PROJECT_DIR}/infra/compose/grafana.yml"
)

cd "${PROJECT_DIR}"

failures=0

print_header() {
  cat <<'REPORT'
Complete platform verification

Check      Status  Detail
-----      ------  ------
REPORT
}

print_result() {
  local icon="$1"
  local check="$2"
  local status="$3"
  local detail="$4"

  printf "%-10s %-7s %s\n" "${icon} ${check}" "${status}" "${detail}"
}

run_check() {
  local check="$1"
  local output
  local detail
  local tmp
  tmp="$(mktemp)"

  if "${@:2}" >"${tmp}" 2>&1; then
    output="$(cat "${tmp}")"
    detail="$(printf '%s\n' "${output}" | tail -n 1)"
    print_result "🟢" "${check}" "ok" "${detail}"
  else
    output="$(cat "${tmp}")"
    detail="$(printf '%s\n' "${output}" | tail -n 1)"
    print_result "🔴" "${check}" "failed" "${detail}"
    printf '\nDetails for failed check: %s\n' "${check}" >&2
    sed 's/^/  /' "${tmp}" >&2
    failures=$((failures + 1))
  fi

  rm -f "${tmp}"
}

running_container() {
  local service="$1"
  docker compose "${COMPOSE_FILES[@]}" ps --status running -q "${service}" 2>/dev/null
}

flink_running_jobs() {
  if [[ -z "$(running_container jobmanager)" ]]; then
    return
  fi

  docker compose "${COMPOSE_FILES[@]}" exec -T jobmanager /opt/flink/bin/flink list -r 2>/dev/null || true
}

require_topic() {
  local topic="$1"
  docker compose "${COMPOSE_FILES[@]}" exec -T redpanda \
    rpk topic describe "${topic}" --brokers localhost:9092 >/dev/null
}

check_m1() {
  python3 -m py_compile streaming/replay_data.py
  [[ -n "$(running_container redpanda)" ]] || { echo "redpanda is not running"; return 1; }
  [[ -n "$(running_container redpanda-console)" ]] || { echo "redpanda-console is not running"; return 1; }
  require_topic clickstream-raw
  require_topic clickstream-clean
  require_topic clickstream-dlq
  echo "replay compiles; broker, console, and topics are available"
}

check_m2() {
  [[ -n "$(running_container jobmanager)" ]] || { echo "jobmanager is not running"; return 1; }
  [[ -n "$(running_container taskmanager)" ]] || { echo "taskmanager is not running"; return 1; }
  flink_running_jobs | grep -q 'm2-clickstream-validation' || { echo "validation Flink job is not running"; return 1; }
  local report
  if ! report="$("${SCRIPT_DIR}/verify_m2.sh")"; then
    printf '%s\n' "${report}"
    return 1
  fi
  local raw clean dlq
  raw="$(printf '%s\n' "${report}" | awk -F= '$1 == "raw_records" { print $2 }')"
  clean="$(printf '%s\n' "${report}" | awk -F= '$1 == "clean_records" { print $2 }')"
  dlq="$(printf '%s\n' "${report}" | awk -F= '$1 == "dlq_records" { print $2 }')"
  echo "validation job running; raw=${raw:-0}, clean=${clean:-0}, dlq=${dlq:-0}"
}

check_m3() {
  local partition_count="0"
  local parquet_file_count="0"
  if [[ -d "${PARQUET_DIR}" ]]; then
    partition_count="$(find "${PARQUET_DIR}" -type d -name 'event_date=*' | wc -l | tr -d '[:space:]')"
    parquet_file_count="$(find "${PARQUET_DIR}" -type f -name 'part-*' ! -name '*.inprogress*' | wc -l | tr -d '[:space:]')"
  fi

  if [[ "${partition_count}" -gt 0 && "${parquet_file_count}" -gt 0 ]]; then
    echo "dataset materialized; partitions=${partition_count}, parquet_files=${parquet_file_count}"
    return
  fi

  flink_running_jobs | grep -q 'm3-clean-clickstream-parquet' || { echo "no M3 dataset and analytics writer is not running"; return 1; }
  echo "analytics writer running; dataset not materialized yet"
}

check_m4() {
  local artifacts=(
    "${ARTIFACT_DIR}/normalization.parquet"
    "${ARTIFACT_DIR}/bot_config.json"
    "${PROJECT_DIR}/data/flink/generated/flink_job_operational.sql.template"
  )

  local artifact
  for artifact in "${artifacts[@]}"; do
    [[ -f "${artifact}" ]] || { echo "missing ${artifact}"; return 1; }
  done

  [[ -n "$(running_container postgres)" ]] || { echo "postgres is not running"; return 1; }
  docker compose "${COMPOSE_FILES[@]}" exec -T postgres \
    psql -U clickstream -d clickstream -At -c "SELECT to_regclass('public.session_bot_scores') IS NOT NULL AND to_regclass('public.stream_bot_metrics') IS NOT NULL;" \
    | tr -d '\r' | grep -q '^t$' || { echo "PostgreSQL operational tables are missing"; return 1; }
  flink_running_jobs | grep -q 'm4-operational-bot-scoring' || { echo "operational Flink job is not running"; return 1; }
  echo "artifacts, PostgreSQL tables, and operational job are available"
}

check_m5() {
  local report
  if ! report="$("${SCRIPT_DIR}/verify_m5.sh")"; then
    printf '%s\n' "${report}"
    return 1
  fi
  local dashboard datasource
  dashboard="$(printf '%s\n' "${report}" | awk -F= '$1 == "dashboard_title" { print $2 }')"
  datasource="$(printf '%s\n' "${report}" | awk -F= '$1 == "datasource_name" { print $2 }')"
  [[ -n "${dashboard}" && -n "${datasource}" ]] || { echo "Grafana dashboard or datasource was not reported"; return 1; }
  echo "dashboard=${dashboard}, datasource=${datasource}"
}

check_m6() {
  local report
  if ! report="$("${SCRIPT_DIR}/verify_m6.sh")"; then
    printf '%s\n' "${report}"
    return 1
  fi
  local running_jobs
  running_jobs="$(printf '%s\n' "${report}" | awk -F'|' '/RUNNING/ { count += 1 } END { print count + 0 }')"
  [[ "${running_jobs}" -ge 3 ]] || { echo "expected at least 3 running Flink jobs, found ${running_jobs}"; return 1; }
  echo "topics, UIs, DLQ, metrics, running_jobs=${running_jobs}"
}

print_header
run_check "M1" check_m1
run_check "M2" check_m2
run_check "M3" check_m3
run_check "M4" check_m4
run_check "M5" check_m5
run_check "M6" check_m6

if [[ "${failures}" -gt 0 ]]; then
  printf '\nverify_all=failed failures=%s\n' "${failures}" >&2
  exit 1
fi

printf '\nverify_all=ok\n'
