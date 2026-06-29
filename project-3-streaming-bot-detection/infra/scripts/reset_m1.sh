#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
COMPOSE_FILE="${PROJECT_DIR}/infra/compose/kafka.yml"

docker compose -f "${COMPOSE_FILE}" down --volumes --remove-orphans

echo "M1 broker state removed."
