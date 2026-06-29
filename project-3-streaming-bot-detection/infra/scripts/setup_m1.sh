#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
COMPOSE_FILE="${PROJECT_DIR}/infra/compose/docker-compose.m1.yml"

docker compose -f "${COMPOSE_FILE}" up -d redpanda
docker compose -f "${COMPOSE_FILE}" up topic-init

echo
echo "M1 broker is ready."
echo "Kafka bootstrap server: localhost:19092"
echo "Topics:"
docker compose -f "${COMPOSE_FILE}" exec -T redpanda rpk topic list --brokers localhost:9092
