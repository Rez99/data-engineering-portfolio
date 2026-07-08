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

docker compose "${COMPOSE_FILES[@]}" down --volumes --remove-orphans

rm -rf "${PROJECT_DIR}/datasets/analytics/clickstream"
rm -rf "${PROJECT_DIR}/datasets/flink-checkpoints"
rm -rf "${PROJECT_DIR}/datasets/flink"
rm -rf "${PROJECT_DIR}/infra/flink/generated"
rm -f "${PROJECT_DIR}/datasets/reference/normalization.parquet"
rm -f "${PROJECT_DIR}/datasets/reference/bot_config.json"

cat <<REPORT
Complete platform reset finished.

Removed:
- Docker containers and volumes for Kafka/Redpanda, Flink, PostgreSQL, Grafana, and Redpanda Console
- generated Parquet output under datasets/analytics/clickstream
- Flink checkpoints and generated Flink SQL
- M4 generated reference artifacts in datasets/reference

Kept:
- source CSV in datasets/source/
- Flink connector jars in infra/flink/lib/
- source code, SQL, Compose files, and Grafana provisioning files
REPORT
