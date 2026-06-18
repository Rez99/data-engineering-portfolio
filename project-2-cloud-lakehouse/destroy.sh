#!/usr/bin/env bash

set -euo pipefail

readonly PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly TERRAFORM_CONTAINER="lakehouse-terraform-destroy"
readonly TERRAFORM_IMAGE="hashicorp/terraform:1.15.6"
readonly ADC_FILE="${PROJECT_DIR}/.credentials/gcloud/application_default_credentials.json"

# Validate local prerequisites.
if [[ ! -f "${ADC_FILE}" ]]; then
  printf 'Missing Google Cloud credentials: %s\n' "${ADC_FILE}" >&2
  exit 1
fi

# Small wrappers keep the teardown flow readable.
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

terraform_state_list() {
  local workdir="$1"

  docker exec \
    --workdir "${workdir}" \
    "${TERRAFORM_CONTAINER}" \
    terraform state list
}

remove_state_if_present() {
  local workdir="$1"
  local resource_address="$2"

  if terraform_state_list "${workdir}" \
    | grep --fixed-strings --line-regexp --quiet "${resource_address}"; then
    printf '   Removing from state: %s\n' "${resource_address}"

    run_terraform \
      "${workdir}" \
      state rm \
      "${resource_address}"
  else
    printf '   Not present in state: %s\n' "${resource_address}"
  fi
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
  --workdir /workspace/terraform \
  "${TERRAFORM_IMAGE}" \
  -c "sleep infinity" >/dev/null

printf '\n🟢 Terraform: Initializing main infrastructure state\n'

run_terraform \
  /workspace/terraform \
  init \
  -input=false

SUPERSET_URL="$(
  terraform_output \
    /workspace/terraform \
    -raw superset_service_url \
    2>/dev/null || true
)"

if [[ -n "${SUPERSET_URL}" ]]; then
  printf '\n🟢 Terraform: Destroying Superset application configuration\n'

  run_terraform \
    /workspace/terraform-superset \
    init \
    -input=false

  run_terraform \
    /workspace/terraform-superset \
    destroy \
    -var="superset_endpoint=${SUPERSET_URL}"
else
  printf '\n🟡 Superset: No service URL found; skipping configuration destroy\n'
fi

printf '\n🟠 Terraform: Forgetting Polaris application objects\n'
printf '   The main infrastructure destroy will remove Polaris Cloud Run,\n'
printf '   Cloud SQL metadata, and the GCS warehouse directly.\n'

run_terraform \
  /workspace/terraform-polaris \
  init \
  -input=false

remove_state_if_present \
  /workspace/terraform-polaris \
  polaris_rest_resource.namespace

remove_state_if_present \
  /workspace/terraform-polaris \
  polaris_rest_resource.catalog_admin_table_write

remove_state_if_present \
  /workspace/terraform-polaris \
  polaris_rest_resource.catalog

printf '\n🟢 Terraform: Destroying underlying GCP infrastructure\n'

run_terraform \
  /workspace/terraform \
  destroy

printf '\n✅ Full teardown completed successfully.\n'
