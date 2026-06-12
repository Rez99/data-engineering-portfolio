#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIPELINE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "Stopping services and deleting local volumes..."

if [[ -f "${PIPELINE_DIR}/docker/airflow/.env" ]]; then
  docker compose \
    -p lakehouse-airflow \
    -f "${PIPELINE_DIR}/docker/airflow/docker-compose.yaml" \
    down --volumes --remove-orphans
fi

if [[ -f "${PIPELINE_DIR}/docker/superset/.env" ]]; then
  docker compose \
    -p lakehouse-superset \
    -f "${PIPELINE_DIR}/docker/superset/docker-compose.yaml" \
    down --volumes --remove-orphans
fi

if [[ -f "${PIPELINE_DIR}/docker/polaris/.env" ]]; then
  docker compose \
    -p lakehouse-polaris \
    -f "${PIPELINE_DIR}/docker/polaris/docker-compose.yaml" \
    down --volumes --remove-orphans
fi

rm -f \
  "${PIPELINE_DIR}/docker/airflow/.env" \
  "${PIPELINE_DIR}/docker/polaris/.env" \
  "${PIPELINE_DIR}/docker/superset/.env"

rm -rf \
  "${PIPELINE_DIR}/docker/airflow/config" \
  "${PIPELINE_DIR}/docker/airflow/logs" \
  "${PIPELINE_DIR}/docker/airflow/plugins"

echo "Local environment reset complete."
