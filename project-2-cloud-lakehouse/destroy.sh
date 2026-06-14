#!/usr/bin/env bash

set -euo pipefail

readonly PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly TERRAFORM_CONTAINER="lakehouse-terraform-destroy"
readonly TERRAFORM_IMAGE="hashicorp/terraform:1.15.6"
readonly ADC_FILE="${PROJECT_DIR}/.credentials/gcloud/application_default_credentials.json"
readonly SETUP_ENV="${PROJECT_DIR}/.credentials/setup.env"

if [[ ! -f "${ADC_FILE}" ]]; then
  printf 'Missing Google Cloud credentials: %s\n' "${ADC_FILE}" >&2
  exit 1
fi

if [[ ! -f "${SETUP_ENV}" ]]; then
  printf 'Missing generated setup credentials: %s\n' "${SETUP_ENV}" >&2
  printf 'Run ./setup.sh before destroying the environment.\n' >&2
  exit 1
fi

set -a
# shellcheck source=/dev/null
source "${SETUP_ENV}"
set +a

export TF_VAR_polaris_database_password="${POLARIS_DATABASE_PASSWORD}"
export TF_VAR_polaris_root_client_secret="${POLARIS_ROOT_CLIENT_SECRET}"

cleanup() {
  docker rm --force "${TERRAFORM_CONTAINER}" >/dev/null 2>&1 || true
}

trap cleanup EXIT
cleanup

docker run --detach \
  --name "${TERRAFORM_CONTAINER}" \
  --entrypoint /bin/sh \
  --volume "${PROJECT_DIR}:/workspace" \
  --volume "${ADC_FILE}:/credentials/gcp.json:ro" \
  --env GOOGLE_APPLICATION_CREDENTIALS=/credentials/gcp.json \
  --env TF_VAR_polaris_database_password \
  --env TF_VAR_polaris_root_client_secret \
  --workdir /workspace/terraform \
  "${TERRAFORM_IMAGE}" \
  -c "sleep infinity" >/dev/null

docker exec --interactive --tty \
  "${TERRAFORM_CONTAINER}" \
  terraform destroy
