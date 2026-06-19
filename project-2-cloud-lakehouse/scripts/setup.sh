#!/usr/bin/env bash

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
readonly TERRAFORM_CONTAINER="lakehouse-terraform"
readonly TERRAFORM_IMAGE="hashicorp/terraform:1.15.6"
readonly GCLOUD_IMAGE="gcr.io/google.com/cloudsdktool/google-cloud-cli:572.0.0-stable"
readonly GCLOUD_CONFIG="${PROJECT_DIR}/.credentials/gcloud"
readonly ADC_FILE="${GCLOUD_CONFIG}/application_default_credentials.json"
readonly LOG_DIR="${PROJECT_DIR}/logs"
readonly TERRAFORM_ARTIFACT_REGISTRY_LOG="${LOG_DIR}/terraform-apply-artifact-registry.jsonl"
readonly TERRAFORM_APPLY_LOG="${LOG_DIR}/terraform-apply-full.jsonl"
readonly TERRAFORM_POLARIS_LOG="${LOG_DIR}/terraform-apply-polaris.jsonl"
readonly TERRAFORM_SUPERSET_LOG="${LOG_DIR}/terraform-apply-superset.jsonl"
readonly SETUP_COMMAND_LOG="${LOG_DIR}/setup-commands.log"
readonly PROJECT_ID="rez-cloud-lakehouse"
readonly REGION="us-central1"
readonly REGISTRY_HOST="${REGION}-docker.pkg.dev"
readonly SUPERSET_IMAGE="${REGISTRY_HOST}/${PROJECT_ID}/superset/superset:dev-amd64"
readonly POLARIS_SERVICE="polaris"
readonly POLARIS_BOOTSTRAP_JOB="polaris-bootstrap"
readonly SUPERSET_SERVICE="superset"
readonly SUPERSET_BOOTSTRAP_JOB="superset-bootstrap"
readonly POLARIS_REALM="POLARIS"
readonly POLARIS_ROOT_CLIENT_ID="admin"
readonly PIPELINE_SUCCESS_SENTINEL="gs://${PROJECT_ID}-lakehouse/ml/xgboost_conversion/metrics/metrics.parquet"
readonly SUPERSET_ADMIN_USERNAME="admin"
readonly SUPERSET_ADMIN_PASSWORD="admin"

# Internal Polaris client secret used only for readiness checks and catalog setup.
readonly POLARIS_ROOT_CLIENT_SECRET="polaris_root"

# Validate local prerequisites.
if [[ ! -f "${ADC_FILE}" ]]; then
  printf 'Missing Google Cloud credentials: %s\n' "${ADC_FILE}" >&2
  exit 1
fi

mkdir -p "${LOG_DIR}"

# Small wrappers keep the orchestration below readable.
run_terraform() {
  local workdir="$1"
  shift

  docker exec \
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
  docker rm --force "${TERRAFORM_CONTAINER}" || true
}

gcloud_cmd() {
  docker run --rm \
    --volume "${GCLOUD_CONFIG}:/config" \
    --env CLOUDSDK_CONFIG=/config \
    "${GCLOUD_IMAGE}" \
    gcloud "$@"
}

get_polaris_url() {
  gcloud_cmd run services describe "${POLARIS_SERVICE}" \
    --region="${REGION}" \
    --project="${PROJECT_ID}" \
    --format='value(status.url)'
}

get_cloud_run_identity_token() {
  gcloud_cmd auth print-identity-token
}

get_superset_url() {
  gcloud_cmd run services describe "${SUPERSET_SERVICE}" \
    --region="${REGION}" \
    --project="${PROJECT_ID}" \
    --format='value(status.url)'
}

superset_admin_ready() {
  local superset_url

  superset_url="$(get_superset_url)"

  curl --silent --fail --output /dev/null \
    --connect-timeout 5 \
    --max-time 10 \
    --header "Content-Type: application/json" \
    --data "{
      \"username\": \"${SUPERSET_ADMIN_USERNAME}\",
      \"password\": \"${SUPERSET_ADMIN_PASSWORD}\",
      \"provider\": \"db\",
      \"refresh\": true
    }" \
    "${superset_url}/api/v1/security/login" 2>/dev/null
}

wait_for_superset_admin() {
  local attempt

  for attempt in {1..30}; do
    if superset_admin_ready; then
      return 0
    fi

    sleep 10
  done

  printf 'Superset admin login did not become ready in time.\n' >&2
  return 1
}

pipeline_outputs_ready() {
  gcloud_cmd storage objects describe "${PIPELINE_SUCCESS_SENTINEL}" \
    --project="${PROJECT_ID}" >/dev/null 2>&1
}

polaris_root_auth_ready() {
  local polaris_url
  local identity_token

  polaris_url="$(get_polaris_url)"
  identity_token="$(get_cloud_run_identity_token)"

  curl --silent --fail --output /dev/null \
    --header "Polaris-Realm: ${POLARIS_REALM}" \
    --header "X-Serverless-Authorization: Bearer ${identity_token}" \
    --user "${POLARIS_ROOT_CLIENT_ID}:${POLARIS_ROOT_CLIENT_SECRET}" \
    --data 'grant_type=client_credentials' \
    --data 'scope=PRINCIPAL_ROLE:ALL' \
    "${polaris_url}/api/catalog/v1/oauth/tokens" 2>/dev/null
}

trap cleanup EXIT
cleanup

# Stage 1: provision the registry early so Docker can push the Superset image.
printf '🟢 Terraform: Start container\n'
docker run --detach \
  --name "${TERRAFORM_CONTAINER}" \
  --entrypoint /bin/sh \
  --volume "${PROJECT_DIR}:/workspace" \
  --volume "${ADC_FILE}:/credentials/gcp.json:ro" \
  --env GOOGLE_APPLICATION_CREDENTIALS=/credentials/gcp.json \
  --workdir /workspace/terraform/main \
  "${TERRAFORM_IMAGE}" \
  -c "sleep infinity" >/dev/null

printf '🟢 Terraform: Provision Artifact Registry\n'
run_terraform /workspace/terraform/main init -input=false >/dev/null 2>&1
run_terraform /workspace/terraform/main apply \
  -json \
  -input=false \
  -auto-approve \
  -target=google_artifact_registry_repository.superset >"${TERRAFORM_ARTIFACT_REGISTRY_LOG}"

printf '🟢 Docker: Build and push Superset image\n'
printf '   - GCloud: Request Artifact Registry access token\n'
access_token="$(gcloud_cmd auth print-access-token)"

printf '   - Docker: Authenticate to Artifact Registry\n'
printf '%s' "${access_token}" |
  docker login \
    --username oauth2accesstoken \
    --password-stdin \
    "${REGISTRY_HOST}"

printf '   - Docker: Build and push Superset image\n'
docker buildx build \
  --platform linux/amd64 \
  --provenance=false \
  --tag "${SUPERSET_IMAGE}" \
  --push \
  "${PROJECT_DIR}/deployment/containers/superset"

# Stage 2: provision the cloud platform and deploy project artifacts.
printf '🟢 Terraform: Provision GCP resources\n'
run_terraform /workspace/terraform/main apply \
  -json \
  -input=false \
  -auto-approve \
  -var="superset_image=${SUPERSET_IMAGE}" >"${TERRAFORM_APPLY_LOG}"

# Stage 3: initialize and configure platform state.
printf '🟢 Polaris: Bootstrap realm and root credentials\n'
if polaris_root_auth_ready; then
  printf '   - Polaris: Root credentials already work; skipping bootstrap\n'
else
  env \
    PROJECT_ID="${PROJECT_ID}" \
    REGION="${REGION}" \
    POLARIS_BOOTSTRAP_JOB="${POLARIS_BOOTSTRAP_JOB}" \
    "${SCRIPT_DIR}/bootstrap-polaris.sh" >>"${SETUP_COMMAND_LOG}" 2>&1
fi

printf '🟢 Terraform: Configure Polaris catalog\n'
POLARIS_URL="$(get_polaris_url)"
run_terraform /workspace/terraform/polaris init -input=false >>"${SETUP_COMMAND_LOG}" 2>&1
run_terraform /workspace/terraform/polaris apply \
  -json \
  -input=false \
  -auto-approve \
  -var="polaris_base_url=${POLARIS_URL}" >"${TERRAFORM_POLARIS_LOG}" 2>&1

printf '🟢 Superset: Bootstrap metadata and admin user\n'
if superset_admin_ready; then
  printf '   - Superset: Admin login already works; skipping bootstrap\n'
else
  env \
    PROJECT_ID="${PROJECT_ID}" \
    REGION="${REGION}" \
    SUPERSET_BOOTSTRAP_JOB="${SUPERSET_BOOTSTRAP_JOB}" \
    "${SCRIPT_DIR}/bootstrap-superset.sh" >>"${SETUP_COMMAND_LOG}" 2>&1
  fi

# Stage 4: run the data pipeline, then publish the dashboard.
printf '🟢 GCloud: Run end-to-end lakehouse pipeline\n'
if pipeline_outputs_ready; then
  printf '   - Pipeline outputs already exist; skipping Spark run\n'
else
  "${SCRIPT_DIR}/run-pipeline.sh"
fi

printf '🟢 Superset: Refresh metrics runtime\n'
gcloud_cmd run services update "${SUPERSET_SERVICE}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --update-env-vars="METRICS_REFRESHED_AT=$(date +%s)" >>"${SETUP_COMMAND_LOG}" 2>&1
wait_for_superset_admin

printf '🟢 Terraform: Configure Superset assets\n'
SUPERSET_URL="$(terraform_output /workspace/terraform/main -raw superset_service_url)"
run_terraform /workspace/terraform/superset init -input=false >>"${SETUP_COMMAND_LOG}" 2>&1
superset_tf_args=(
  apply
  -json
  -input=false
  -auto-approve
  -parallelism=1
  "-var=superset_endpoint=${SUPERSET_URL}"
)
run_terraform /workspace/terraform/superset "${superset_tf_args[@]}" >"${TERRAFORM_SUPERSET_LOG}" 2>&1

printf '🟢 Setup complete\n'
printf '\nSuperset login:\n'
printf '  URL      : %s\n' "${SUPERSET_URL}"
printf '  Username : %s\n' "${SUPERSET_ADMIN_USERNAME}"
printf '  Password : %s\n' "${SUPERSET_ADMIN_PASSWORD}"
