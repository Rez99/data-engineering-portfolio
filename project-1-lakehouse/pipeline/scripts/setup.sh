#!/usr/bin/env bash

set -Eeuo pipefail
trap 'echo "Setup failed at line ${LINENO}: ${BASH_COMMAND}" >&2' ERR

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIPELINE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

POLARIS_DIR="${PIPELINE_DIR}/docker/polaris"
AIRFLOW_DIR="${PIPELINE_DIR}/docker/airflow"
SUPERSET_DIR="${PIPELINE_DIR}/docker/superset"

POLARIS_COMPOSE=(docker compose -p lakehouse-polaris -f "${POLARIS_DIR}/docker-compose.yaml")
AIRFLOW_COMPOSE=(docker compose -p lakehouse-airflow -f "${AIRFLOW_DIR}/docker-compose.yaml")
SUPERSET_COMPOSE=(docker compose -p lakehouse-superset -f "${SUPERSET_DIR}/docker-compose.yaml")

DAG_ID="lakehouse_0_pipeline"
RUN_ID="manual_$(date +%s)"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Required command not found: $1" >&2
    exit 1
  fi
}

wait_for_docker() {
  if docker info >/dev/null 2>&1; then
    return
  fi

  if [[ "$(uname -s)" == "Darwin" ]]; then
    echo "Starting Docker Desktop..."
    open -a Docker
  fi

  for attempt in {1..60}; do
    if docker info >/dev/null 2>&1; then
      return
    fi
    sleep 2
  done

  echo "Docker did not become ready." >&2
  exit 1
}

create_environment_files() {
  if [[ ! -f "${POLARIS_DIR}/.env" ]]; then
    cp "${POLARIS_DIR}/.env.example" "${POLARIS_DIR}/.env"
  fi

  if [[ ! -f "${SUPERSET_DIR}/.env" ]]; then
    local secret_key
    local database_password
    secret_key="$(openssl rand -hex 42)"
    database_password="$(openssl rand -hex 24)"

    cat >"${SUPERSET_DIR}/.env" <<EOF
SUPERSET_SECRET_KEY=${secret_key}
SUPERSET_DATABASE_PASSWORD=${database_password}
SUPERSET_DATABASE_URI=postgresql+psycopg2://superset:${database_password}@superset-db:5432/superset
SUPERSET_ADMIN_USERNAME=admin
SUPERSET_ADMIN_PASSWORD=admin
SUPERSET_ADMIN_FIRSTNAME=Superset
SUPERSET_ADMIN_LASTNAME=Admin
SUPERSET_ADMIN_EMAIL=admin@example.com
RUSTFS_ACCESS_KEY=polaris_root
RUSTFS_SECRET_KEY=polaris_pass
EOF
  fi
}

create_airflow_environment() {
  set -a
  source "${POLARIS_DIR}/.env"
  set +a

  cat >"${AIRFLOW_DIR}/.env" <<EOF
AIRFLOW_UID=$(id -u)
FERNET_KEY=
AIRFLOW_CLIENT_ID=${AIRFLOW_CLIENT_ID}
AIRFLOW_CLIENT_SECRET=${AIRFLOW_CLIENT_SECRET}
RUSTFS_ACCESS_KEY=${RUSTFS_ACCESS_KEY}
RUSTFS_SECRET_KEY=${RUSTFS_SECRET_KEY}
EOF
}

wait_for_dag() {
  echo "Waiting for Airflow to discover ${DAG_ID}..."

  for attempt in {1..60}; do
    if "${AIRFLOW_COMPOSE[@]}" exec -T airflow-worker \
      airflow dags list 2>/dev/null |
      grep -q "^${DAG_ID}[[:space:]]"; then
      return
    fi
    sleep 2
  done

  echo "Airflow did not discover ${DAG_ID}." >&2
  exit 1
}

run_pipeline() {
  echo "Triggering ${DAG_ID}..."
  "${AIRFLOW_COMPOSE[@]}" exec -T airflow-worker \
    airflow dags trigger "${DAG_ID}" --run-id "${RUN_ID}" >/dev/null

  local token
  token="$(
    curl --fail --silent --show-error \
      -X POST http://localhost:8080/auth/token \
      -H "Content-Type: application/json" \
      -d '{"username":"airflow","password":"airflow"}' |
      jq -er '.access_token'
  )"

  for attempt in {1..240}; do
    local state
    state="$(
      curl --fail --silent --show-error \
        -H "Authorization: Bearer ${token}" \
        "http://localhost:8080/api/v2/dags/${DAG_ID}/dagRuns/${RUN_ID}" |
        jq -r '.state'
    )"

    case "${state}" in
      success)
        echo "Airflow pipeline complete."
        return
        ;;
      failed)
        echo "Airflow pipeline failed. See http://localhost:8080." >&2
        exit 1
        ;;
      *)
        echo "Pipeline state: ${state}"
        sleep 15
        ;;
    esac
  done

  echo "Pipeline wait timed out after one hour." >&2
  exit 1
}

for command in docker curl jq openssl; do
  require_command "${command}"
done

wait_for_docker
create_environment_files
mkdir -p "${AIRFLOW_DIR}/logs" "${AIRFLOW_DIR}/plugins" "${AIRFLOW_DIR}/config"

echo "Starting RustFS and Polaris..."
"${POLARIS_COMPOSE[@]}" up -d --wait >/dev/null
"${POLARIS_DIR}/provision.sh"
create_airflow_environment

echo "Initializing Airflow..."
"${AIRFLOW_COMPOSE[@]}" up airflow-init --build >/dev/null

echo "Starting Airflow and Superset..."
"${AIRFLOW_COMPOSE[@]}" up -d --build --wait >/dev/null
"${SUPERSET_COMPOSE[@]}" up -d --build --wait superset >/dev/null

wait_for_dag
run_pipeline

echo "Registering ML metrics and dashboard assets in Superset..."
"${SUPERSET_COMPOSE[@]}" run --rm --no-deps superset-assets

cat <<'EOF'

Lakehouse setup complete.

Airflow:  http://localhost:8080  (airflow / airflow)
RustFS:   http://localhost:9001
Superset: http://localhost:8088  (admin / admin)
Dashboard: http://localhost:8088/superset/dashboard/xgboost-model-evaluation/
EOF
