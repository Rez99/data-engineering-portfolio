#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ARTIFACT_DIR="${PROJECT_DIR}/batch/artifacts"
COMPOSE_FILES=(
  -f "${PROJECT_DIR}/infra/compose/kafka.yml"
  -f "${PROJECT_DIR}/infra/compose/kafka-ui.yml"
  -f "${PROJECT_DIR}/infra/compose/flink.yml"
  -f "${PROJECT_DIR}/infra/compose/postgres.yml"
)

docker compose "${COMPOSE_FILES[@]}" stop validation-job operational-job postgres || true
docker compose "${COMPOSE_FILES[@]}" rm -f -v validation-job operational-job postgres || true
docker volume rm project-3-streaming-bot-detection-m1_postgres-data >/dev/null 2>&1 || true
rm -f "${ARTIFACT_DIR}/normalization.parquet"
rm -f "${ARTIFACT_DIR}/bot_config.json"
rm -rf "${PROJECT_DIR}/data/flink/checkpoints/m4-operational"
rm -rf "${PROJECT_DIR}/data/flink/generated"

echo "Removed M4 Flink/PostgreSQL state and generated artifacts from ${ARTIFACT_DIR}."
