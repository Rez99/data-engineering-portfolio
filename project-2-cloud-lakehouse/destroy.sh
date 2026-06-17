#!/usr/bin/env bash

set -euo pipefail

readonly PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly TERRAFORM_CONTAINER="lakehouse-terraform-destroy"
readonly TERRAFORM_IMAGE="hashicorp/terraform:1.15.6"
readonly ADC_FILE="${PROJECT_DIR}/.credentials/gcloud/application_default_credentials.json"
readonly SETUP_ENV="${PROJECT_DIR}/.credentials/setup.env"
readonly PROJECT_ID="rez-cloud-lakehouse"
readonly POLARIS_WAREHOUSE="gs://${PROJECT_ID}-validation/warehouse/"
readonly POLARIS_SERVICE_ACCOUNT="lakehouse-polaris@${PROJECT_ID}.iam.gserviceaccount.com"
readonly SUPERSET_ADMIN_PASSWORD="admin"

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
export TF_VAR_superset_database_password="${SUPERSET_DATABASE_PASSWORD}"
export TF_VAR_superset_secret_key="${SUPERSET_SECRET_KEY}"
export TF_VAR_superset_admin_password="${SUPERSET_ADMIN_PASSWORD}"

run_terraform() {
  local workdir="$1"
  shift

  docker exec --interactive --tty \
    --workdir "${workdir}" \
    "${TERRAFORM_CONTAINER}" \
    terraform "$@"
}

terraform_output() {
  local workdir="$1"
  shift

  docker exec \
    --workdir "${workdir}" \
    "${TERRAFORM_CONTAINER}" \
    terraform output "$@"
}

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
  --env TF_VAR_superset_database_password \
  --env TF_VAR_superset_secret_key \
  --env TF_VAR_superset_admin_password \
  --workdir /workspace/terraform \
  "${TERRAFORM_IMAGE}" \
  -c "sleep infinity" >/dev/null

run_terraform /workspace/terraform init -input=false
POLARIS_URL="$(terraform_output /workspace/terraform -raw polaris_service_url 2>/dev/null || true)"
SUPERSET_URL="$(terraform_output /workspace/terraform -raw superset_service_url 2>/dev/null || true)"

if [[ -n "${SUPERSET_URL}" ]]; then
  run_terraform /workspace/terraform-superset init -input=false
  run_terraform /workspace/terraform-superset destroy \
    -var="superset_endpoint=${SUPERSET_URL}" \
    -var="superset_password=${SUPERSET_ADMIN_PASSWORD}"
fi

if [[ -n "${POLARIS_URL}" ]]; then
  run_terraform /workspace/terraform-polaris init -input=false
  run_terraform /workspace/terraform-polaris destroy \
    -var="polaris_base_url=${POLARIS_URL}" \
    -var="polaris_root_client_secret=${POLARIS_ROOT_CLIENT_SECRET}" \
    -var="warehouse_location=${POLARIS_WAREHOUSE}" \
    -var="gcs_service_account=${POLARIS_SERVICE_ACCOUNT}"
fi

run_terraform /workspace/terraform destroy
