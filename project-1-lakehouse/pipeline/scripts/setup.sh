#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIPELINE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_DIR="${PIPELINE_DIR}/logs"
LOG_FILE="${LOG_DIR}/setup.log"

POLARIS_DIR="${PIPELINE_DIR}/docker/polaris"
AIRFLOW_DIR="${PIPELINE_DIR}/docker/airflow"
SUPERSET_DIR="${PIPELINE_DIR}/docker/superset"

POLARIS_COMPOSE=(docker compose -p lakehouse-polaris -f "${POLARIS_DIR}/docker-compose.yaml")
AIRFLOW_COMPOSE=(docker compose -p lakehouse-airflow -f "${AIRFLOW_DIR}/docker-compose.yaml")
SUPERSET_COMPOSE=(docker compose -p lakehouse-superset -f "${SUPERSET_DIR}/docker-compose.yaml")

DAG_ID="lakehouse_0_pipeline"
RUN_ID="manual_$(date +%s)"
TOTAL_STEPS=6
USE_IN_PLACE_STATUS=false

if [[ -t 1 && "${TERM:-dumb}" != "dumb" ]]; then
  USE_IN_PLACE_STATUS=true
fi

log_info() {
  printf '[%s] INFO  %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" >>"${LOG_FILE}"
}

log_success() {
  printf '[%s] OK    %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" >>"${LOG_FILE}"
}

log_error() {
  printf '[%s] ERROR %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" >>"${LOG_FILE}"
}

print_step_start() {
  local step_number="$1"
  local message="$2"
  local line
  line="[$step_number/$TOTAL_STEPS] $message"

  printf '%-62s' "${line}"
}

print_step_result() {
  local step_number="$1"
  local message="$2"
  local result="$3"
  local line
  line="[$step_number/$TOTAL_STEPS] $message"

  if [[ "${USE_IN_PLACE_STATUS}" == "true" ]]; then
    printf '\r\033[2K%-62s %s\n' "${line}" "${result}"
  else
    printf ' %s\n' "${result}"
  fi
}

show_failure() {
  local step_number="$1"
  local message="$2"

  print_step_result "${step_number}" "${message}" "❌ Failed"
  printf '\nDetailed logs: %s\n\n' "${LOG_FILE}" >&2
  printf '%s\n' 'Last 50 log lines:' >&2
  printf '%s\n' '────────────────────────────────────────' >&2
  tail -n 50 "${LOG_FILE}" >&2
}

run_step() {
  local step_number="$1"
  local message="$2"
  local success_status="$3"
  shift 3

  print_step_start "${step_number}" "${message}"
  log_info "Step ${step_number}/${TOTAL_STEPS}: ${message}"

  if (set -Eeuo pipefail; "$@") >>"${LOG_FILE}" 2>&1; then
    log_success "${message}"
    print_step_result "${step_number}" "${message}" "✅ ${success_status}"
    return
  fi

  log_error "${message}"
  show_failure "${step_number}" "${message}"
  exit 1
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Required command not found: $1" >&2
    return 1
  fi
}

wait_for_docker() {
  if docker info >/dev/null 2>&1; then
    return
  fi

  if [[ "$(uname -s)" == "Darwin" ]]; then
    log_info "Starting Docker Desktop"
    open -a Docker
  fi

  for attempt in {1..60}; do
    if docker info >/dev/null 2>&1; then
      return
    fi
    sleep 2
  done

  echo "Docker did not become ready." >&2
  return 1
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
  log_info "Waiting for Airflow to discover ${DAG_ID}"

  for attempt in {1..60}; do
    if "${AIRFLOW_COMPOSE[@]}" exec -T airflow-worker \
      airflow dags list 2>/dev/null |
      grep -q "^${DAG_ID}[[:space:]]"; then
      return
    fi
    sleep 2
  done

  echo "Airflow did not discover ${DAG_ID}." >&2
  return 1
}

run_pipeline() {
  log_info "Triggering ${DAG_ID} with run ID ${RUN_ID}"
  "${AIRFLOW_COMPOSE[@]}" exec -T airflow-worker \
    airflow dags trigger "${DAG_ID}" --run-id "${RUN_ID}"

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
        log_success "Airflow pipeline ${RUN_ID} completed"
        return
        ;;
      failed)
        echo "Airflow pipeline failed. See http://localhost:8080." >&2
        return 1
        ;;
      *)
        log_info "Pipeline state: ${state}"
        sleep 15
        ;;
    esac
  done

  echo "Pipeline wait timed out after one hour." >&2
  return 1
}

start_rustfs_and_polaris() {
  for command in docker curl jq openssl; do
    require_command "${command}" || return 1
  done

  wait_for_docker || return 1
  create_environment_files || return 1
  mkdir -p \
    "${AIRFLOW_DIR}/logs" \
    "${AIRFLOW_DIR}/plugins" \
    "${AIRFLOW_DIR}/config" || return 1
  "${POLARIS_COMPOSE[@]}" up -d --wait
}

configure_polaris() {
  "${POLARIS_DIR}/provision.sh" || return 1
  create_airflow_environment
}

initialize_airflow() {
  "${AIRFLOW_COMPOSE[@]}" up airflow-init --build
}

start_airflow_and_superset() {
  "${AIRFLOW_COMPOSE[@]}" up -d --build --wait || return 1
  "${SUPERSET_COMPOSE[@]}" up -d --build --wait superset || return 1
  wait_for_dag
}

register_dashboard_assets() {
  "${SUPERSET_COMPOSE[@]}" run --rm --no-deps superset-assets
}

mkdir -p "${LOG_DIR}"
: >"${LOG_FILE}"

printf '\n🚀 Lakehouse Setup\n'
printf '────────────────────────────────────────\n'

run_step 1 "🪣 Starting RustFS + Polaris..." "Ready" start_rustfs_and_polaris
run_step 2 "🔐 Configuring Polaris..." "Complete" configure_polaris
run_step 3 "🌬️  Initializing Airflow..." "Ready" initialize_airflow
run_step 4 "📊 Starting Airflow + Superset..." "Ready" start_airflow_and_superset
run_step 5 "▶️  Running sample pipeline..." "Complete" run_pipeline
run_step 6 "📈 Registering dashboard assets..." "Complete" register_dashboard_assets

cat <<EOF

🎉 Lakehouse setup complete!

✓ Airflow:   http://localhost:8080
✓ Superset:  http://localhost:8088
✓ RustFS:    http://localhost:9001

Credentials:
  Airflow  : airflow / airflow
  Superset : admin / admin

Dashboard:
  http://localhost:8088/superset/dashboard/xgboost-model-evaluation/

Detailed log:
  ${LOG_FILE}
EOF
