#!/usr/bin/env bash

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
readonly TERRAFORM_DIR="${PROJECT_DIR}/terraform"
readonly TERRAFORM_IMAGE="hashicorp/terraform:1.15.6"
readonly GCLOUD_ADC="${PROJECT_DIR}/.credentials/gcloud/application_default_credentials.json"

docker_args=(
  run
  --rm
  --volume "${TERRAFORM_DIR}:/workspace"
  --workdir /workspace
)

if [[ -t 0 && -t 1 ]]; then
  docker_args+=(--interactive --tty)
fi

if [[ -n "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]]; then
  if [[ ! -f "${GOOGLE_APPLICATION_CREDENTIALS}" ]]; then
    printf 'Credential file not found: %s\n' "${GOOGLE_APPLICATION_CREDENTIALS}" >&2
    exit 1
  fi

  docker_args+=(
    --env GOOGLE_APPLICATION_CREDENTIALS=/credentials/gcp.json
    --volume "${GOOGLE_APPLICATION_CREDENTIALS}:/credentials/gcp.json:ro"
  )
elif [[ -f "${GCLOUD_ADC}" ]]; then
  docker_args+=(
    --env GOOGLE_APPLICATION_CREDENTIALS=/credentials/application_default_credentials.json
    --volume "${GCLOUD_ADC}:/credentials/application_default_credentials.json:ro"
  )
fi

docker "${docker_args[@]}" "${TERRAFORM_IMAGE}" "$@"
