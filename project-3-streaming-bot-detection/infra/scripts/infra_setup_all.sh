#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "${PROJECT_DIR}"

run_milestone_setup() {
  local milestone="$1"
  local current="$2"
  local total="$3"
  local script="${SCRIPT_DIR}/infra_setup_${milestone}.sh"
  local label
  local log_file

  if [[ ! -x "${script}" ]]; then
    echo "Missing or non-executable milestone setup script: ${script}" >&2
    exit 1
  fi

  case "${milestone}" in
    m1) label="M1 broker + Kafka topics" ;;
    m2) label="M2 validation job" ;;
    m3) label="M3 Parquet writer" ;;
    m4) label="M4 bot scoring + Postgres" ;;
    m5) label="M5 Grafana dashboard" ;;
    m6) label="M6 observability mode" ;;
    *) label="${milestone}" ;;
  esac

  log_file="$(mktemp)"
  printf '[%s/%s] %s ... ' "${current}" "${total}" "${label}"

  if "${script}" >"${log_file}" 2>&1; then
    printf 'ok\n'
  else
    printf 'failed\n\n'
    echo "Output from ${script}:"
    sed 's/^/  /' "${log_file}"
    rm -f "${log_file}"
    exit 1
  fi

  rm -f "${log_file}"
}

milestones=(m1 m2 m3 m4 m5 m6)
total="${#milestones[@]}"

for index in "${!milestones[@]}"; do
  run_milestone_setup "${milestones[$index]}" "$((index + 1))" "${total}"
done

printf '\nComplete streaming platform is available.\n\n'
printf 'Redpanda Console: http://localhost:8080\n'
printf 'Flink Web UI: http://localhost:8081\n'
printf 'Grafana: http://localhost:3000\n'
