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

detail=()

add_detail() {
  detail+=("$1")
}

running_container() {
  local service="$1"
  docker compose "${COMPOSE_FILES[@]}" ps --status running -q "${service}" 2>/dev/null
}

topic_exists() {
  local topic="$1"
  docker compose "${COMPOSE_FILES[@]}" exec -T redpanda \
    rpk topic describe "${topic}" --brokers localhost:9092 >/dev/null 2>&1
}

topic_has_records() {
  local topic="$1"
  local count

  count="$(
    docker compose "${COMPOSE_FILES[@]}" exec -T redpanda \
      bash -lc "timeout 3 rpk topic consume ${topic} --brokers localhost:9092 --offset start --num 1 --format '%v\\n' | wc -l" \
      2>/dev/null | tr -d '[:space:]'
  )"

  [[ "${count:-0}" -gt 0 ]]
}

flink_running_jobs() {
  docker compose "${COMPOSE_FILES[@]}" exec -T jobmanager \
    /opt/flink/bin/flink list -r 2>/dev/null || true
}

postgres_scalar() {
  local sql="$1"
  docker compose "${COMPOSE_FILES[@]}" exec -T postgres \
    psql -U clickstream -d clickstream -At -c "${sql}" 2>/dev/null | tr -d '\r'
}

platform_ready() {
  local service
  local topic
  local jobs
  local tables_exist
  local required_services=(redpanda redpanda-console jobmanager taskmanager postgres grafana)
  local required_topics=(clickstream-raw clickstream-clean clickstream-dlq)

  for service in "${required_services[@]}"; do
    if [[ -z "$(running_container "${service}")" ]]; then
      add_detail "Service is not running: ${service}"
      return 1
    fi
  done

  for topic in "${required_topics[@]}"; do
    if ! topic_exists "${topic}"; then
      add_detail "Kafka topic is missing: ${topic}"
      return 1
    fi
  done

  jobs="$(flink_running_jobs)"
  if ! grep -q 'm2-clickstream-validation' <<<"${jobs}"; then
    add_detail "Flink job is not running: m2-clickstream-validation"
    return 1
  fi

  if ! grep -q 'm6-analytics-observer' <<<"${jobs}"; then
    add_detail "Flink job is not running: m6-analytics-observer"
    return 1
  fi

  if [[ -f "${PROJECT_DIR}/batch/artifacts/bot_config.json" ]] \
    && [[ -f "${PROJECT_DIR}/data/flink/generated/normalization_values.csv" ]] \
    && [[ ! -f "${PROJECT_DIR}/data/flink/generated/operational-bot-scoring.jar" ]]; then
    add_detail "M4 artifacts exist, but data/flink/generated/operational-bot-scoring.jar is missing."
    return 1
  fi

  if [[ -f "${PROJECT_DIR}/batch/artifacts/bot_config.json" ]] \
    && [[ -f "${PROJECT_DIR}/data/flink/generated/normalization_values.csv" ]] \
    && [[ -f "${PROJECT_DIR}/data/flink/generated/operational-bot-scoring.jar" ]] \
    && ! grep -q 'm4-operational-bot-scoring' <<<"${jobs}"; then
    add_detail "M4 artifacts exist, but m4-operational-bot-scoring is not running."
    return 1
  fi

  tables_exist="$(postgres_scalar "SELECT to_regclass('public.session_bot_scores') IS NOT NULL AND to_regclass('public.stream_bot_metrics') IS NOT NULL;")"
  if [[ "${tables_exist}" != "t" ]]; then
    add_detail "PostgreSQL operational tables are missing."
    return 1
  fi

  add_detail "Core services, Kafka topics, Flink listeners, and PostgreSQL tables are available."
}

data_present() {
  local topic
  local session_rows
  local metric_rows

  for topic in clickstream-raw clickstream-clean clickstream-dlq; do
    if topic_has_records "${topic}"; then
      add_detail "Kafka topic has replay records: ${topic}"
      return 0
    fi
  done

  session_rows="$(postgres_scalar "SELECT COUNT(*) FROM session_bot_scores;" || true)"
  metric_rows="$(postgres_scalar "SELECT COUNT(*) FROM stream_bot_metrics;" || true)"
  session_rows="${session_rows:-0}"
  metric_rows="${metric_rows:-0}"

  if [[ "${session_rows}" -gt 0 || "${metric_rows}" -gt 0 ]]; then
    add_detail "PostgreSQL has data rows: session_bot_scores=${session_rows}, stream_bot_metrics=${metric_rows}"
    return 0
  fi

  add_detail "No replay records were found in Kafka or PostgreSQL."
  return 1
}

print_details() {
  local item

  for item in "${detail[@]}"; do
    printf -- "- %s\n" "${item}"
  done
}

if platform_ready; then
  if data_present; then
    cat <<'REPORT'
State: Data Present

Next valid action:
  ./infra/scripts/reset_data.sh

Other valid action:
  ./infra/scripts/reset_all_infra.sh

Why:
REPORT
    print_details
  else
    cat <<'REPORT'
State: Platform Ready

Next valid action:
  python3 streaming/replay_data.py --start-row 100000 --rows 1000000 --speed 100x --sink kafka --corrupt-probability 0.02 --delay-probability 0.02 --quiet --progress-every 100000

Other valid action:
  ./infra/scripts/reset_all_infra.sh

Why:
REPORT
    print_details
  fi
else
  cat <<'REPORT'
State: Start

Next valid action:
  ./infra/scripts/setup_all_infra.sh

Why:
REPORT
  print_details
fi
