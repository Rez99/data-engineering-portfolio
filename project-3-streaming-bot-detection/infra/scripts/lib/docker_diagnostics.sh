#!/usr/bin/env bash

docker_error_is_environmental() {
  local output_file="$1"

  grep -Eiq \
    'docker API|docker\.sock|permission denied|Cannot connect to the Docker daemon|is the docker daemon running' \
    "${output_file}"
}

docker_check_or_explain() {
  local failure_context="$1"
  shift

  local output_file
  output_file="$(mktemp)"

  local status
  if "$@" >/dev/null 2>"${output_file}"; then
    rm -f "${output_file}"
    return 0
  else
    status=$?
  fi

  if docker_error_is_environmental "${output_file}"; then
    echo "${failure_context}: Docker is unavailable from this process." >&2
    echo "Underlying Docker error:" >&2
    sed 's/^/  /' "${output_file}" >&2
    rm -f "${output_file}"
    exit "${status}"
  fi

  rm -f "${output_file}"
  return "${status}"
}

docker_output_or_explain() {
  local failure_context="$1"
  shift

  local output_file
  output_file="$(mktemp)"

  local status
  if "$@" 2>"${output_file}"; then
    rm -f "${output_file}"
    return 0
  else
    status=$?
  fi

  if docker_error_is_environmental "${output_file}"; then
    echo "${failure_context}: Docker is unavailable from this process." >&2
    echo "Underlying Docker error:" >&2
    sed 's/^/  /' "${output_file}" >&2
    rm -f "${output_file}"
    exit "${status}"
  fi

  rm -f "${output_file}"
  return "${status}"
}
