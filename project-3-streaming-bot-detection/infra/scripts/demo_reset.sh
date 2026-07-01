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
PARQUET_DIR="${PROJECT_DIR}/data/analytics/clickstream"
CHECKPOINT_DIR="${PROJECT_DIR}/data/flink/checkpoints"

cd "${PROJECT_DIR}"

rpk() {
  docker compose "${COMPOSE_FILES[@]}" exec -T redpanda rpk "$@" --brokers localhost:9092
}

cancel_running_flink_jobs() {
  if [[ -z "$(docker compose "${COMPOSE_FILES[@]}" ps --status running -q jobmanager 2>/dev/null)" ]]; then
    return
  fi

  docker compose "${COMPOSE_FILES[@]}" exec -T jobmanager bash -lc \
    "/opt/flink/bin/flink list -r | awk '/ : / {print \$4}' | xargs -r -n1 /opt/flink/bin/flink cancel" || true
}

delete_topic_if_exists() {
  local topic="$1"

  if rpk topic describe "${topic}" >/dev/null 2>&1; then
    rpk topic delete "${topic}" >/dev/null
  fi

  for _ in {1..30}; do
    if ! rpk topic describe "${topic}" >/dev/null 2>&1; then
      return
    fi
    sleep 1
  done

  echo "Timed out waiting for topic deletion: ${topic}" >&2
  exit 1
}

create_topic() {
  local topic="$1"
  local partitions="$2"
  local retention_ms="$3"

  rpk topic create "${topic}" \
    --partitions "${partitions}" \
    --replicas 1 \
    --topic-config "retention.ms=${retention_ms}" \
    --topic-config "cleanup.policy=delete" >/dev/null
}

echo "Ensuring infrastructure containers are running..."
docker compose "${COMPOSE_FILES[@]}" up -d \
  redpanda redpanda-console jobmanager taskmanager postgres grafana

echo "Temporarily canceling Flink job listeners before deleting Kafka topics..."
cancel_running_flink_jobs
docker compose "${COMPOSE_FILES[@]}" rm -f \
  validation-job analytics-job analytics-observer-job operational-job >/dev/null 2>&1 || true

echo "Resetting Kafka topics..."
delete_topic_if_exists clickstream-raw
delete_topic_if_exists clickstream-clean
delete_topic_if_exists clickstream-dlq
create_topic clickstream-raw 3 604800000
create_topic clickstream-clean 3 604800000
create_topic clickstream-dlq 1 1209600000

echo "Removing generated Parquet output..."
rm -rf "${PARQUET_DIR}"
mkdir -p "${PROJECT_DIR}/data/analytics"

echo "Clearing PostgreSQL operational tables..."
docker compose "${COMPOSE_FILES[@]}" exec -T postgres \
  psql -U clickstream -d clickstream -f /dev/stdin < "${PROJECT_DIR}/sql/postgres_schema.sql"
docker compose "${COMPOSE_FILES[@]}" exec -T postgres \
  psql -U clickstream -d clickstream -c "TRUNCATE TABLE session_bot_scores, stream_bot_metrics;"

echo "Clearing Flink checkpoints..."
rm -rf "${CHECKPOINT_DIR}"
mkdir -p "${CHECKPOINT_DIR}"

echo "Resubmitting live platform Flink job listeners..."
docker compose "${COMPOSE_FILES[@]}" up -d --force-recreate validation-job

if [[ -f "${PROJECT_DIR}/data/flink/generated/flink_job_operational.sql.template" ]] \
  && [[ -f "${PROJECT_DIR}/batch/artifacts/bot_config.json" ]]; then
  docker compose "${COMPOSE_FILES[@]}" up -d --force-recreate operational-job
else
  echo "Skipping operational-job restart because M4 generated artifacts are missing."
fi

docker compose "${COMPOSE_FILES[@]}" up -d --force-recreate analytics-observer-job

cat <<REPORT
Demo data reset finished.

Kept:
- Docker infrastructure containers
- source CSV in data/source/
- Flink connector jars in data/flink/lib/
- M4 reference artifacts in batch/artifacts/
- Grafana provisioning files

Cleared:
- Kafka topic contents for clickstream-raw, clickstream-clean, clickstream-dlq
- Parquet output under data/analytics/clickstream
- PostgreSQL rows in session_bot_scores and stream_bot_metrics
- Flink checkpoints under data/flink/checkpoints
REPORT
