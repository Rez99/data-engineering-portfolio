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

topic_count() {
  local topic="$1"

  docker compose "${COMPOSE_FILES[@]}" exec -T redpanda \
    bash -lc "timeout 3 rpk topic consume ${topic} --brokers localhost:9092 --offset start --num 1000 --format '%v\\n' | wc -l" \
    | tr -d '[:space:]'
}

raw_count="$(topic_count clickstream-raw)"
clean_count="$(topic_count clickstream-clean)"
dlq_count="$(topic_count clickstream-dlq)"
validated_count=$((clean_count + dlq_count))

if [[ "${validated_count}" -gt 0 ]]; then
  dlq_rate="$(awk -v dlq="${dlq_count}" -v total="${validated_count}" 'BEGIN { printf "%.2f", (dlq / total) * 100 }')"
else
  dlq_rate="0.00"
fi

cat <<REPORT
M2 validation metrics
raw_records=${raw_count}
clean_records=${clean_count}
dlq_records=${dlq_count}
validated_records=${validated_count}
dlq_rate_percent=${dlq_rate}
REPORT
