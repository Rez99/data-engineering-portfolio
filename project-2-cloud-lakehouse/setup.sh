#!/usr/bin/env bash

set -euo pipefail

readonly PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly TERRAFORM_CONTAINER="lakehouse-terraform"
readonly TERRAFORM_IMAGE="hashicorp/terraform:1.15.6"
readonly GCLOUD_IMAGE="gcr.io/google.com/cloudsdktool/google-cloud-cli:572.0.0-stable"
readonly GCLOUD_CONFIG="${PROJECT_DIR}/.credentials/gcloud"
readonly ADC_FILE="${GCLOUD_CONFIG}/application_default_credentials.json"
readonly SETUP_ENV="${PROJECT_DIR}/.credentials/setup.env"
readonly LOG_DIR="${PROJECT_DIR}/logs"
readonly TERRAFORM_APPLY_1_LOG="${LOG_DIR}/terraform-apply-1-foundation.jsonl"
readonly TERRAFORM_APPLY_2_LOG="${LOG_DIR}/terraform-apply-2-images.jsonl"
readonly PROJECT_ID="rez-cloud-lakehouse"
readonly REGION="us-central1"
readonly REGISTRY_HOST="${REGION}-docker.pkg.dev"
readonly INGESTION_IMAGE="${REGISTRY_HOST}/${PROJECT_ID}/pipeline/ingestion:dev-amd64"
readonly ML_IMAGE="${REGISTRY_HOST}/${PROJECT_ID}/pipeline/ml:dev-amd64"
readonly SUPERSET_IMAGE="${REGISTRY_HOST}/${PROJECT_ID}/pipeline/superset:dev-amd64"
readonly POLARIS_SERVICE="lakehouse-polaris"
readonly POLARIS_BOOTSTRAP_JOB="lakehouse-polaris-bootstrap"
readonly POLARIS_WAREHOUSE="gs://${PROJECT_ID}-validation/warehouse/"
readonly POLARIS_SERVICE_ACCOUNT="lakehouse-polaris@${PROJECT_ID}.iam.gserviceaccount.com"

if [[ ! -f "${ADC_FILE}" ]]; then
  printf 'Missing Google Cloud credentials: %s\n' "${ADC_FILE}" >&2
  exit 1
fi

provided_database_password="${POLARIS_DATABASE_PASSWORD:-}"
provided_root_client_secret="${POLARIS_ROOT_CLIENT_SECRET:-}"
provided_superset_database_password="${SUPERSET_DATABASE_PASSWORD:-}"
provided_superset_secret_key="${SUPERSET_SECRET_KEY:-}"

unset POLARIS_DATABASE_PASSWORD POLARIS_ROOT_CLIENT_SECRET
unset SUPERSET_DATABASE_PASSWORD SUPERSET_SECRET_KEY SUPERSET_ADMIN_PASSWORD

umask 077
mkdir -p "$(dirname "${SETUP_ENV}")"
mkdir -p "${LOG_DIR}"

if [[ -f "${SETUP_ENV}" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "${SETUP_ENV}"
  set +a
else
  touch "${SETUP_ENV}"
fi

if [[ -z "${POLARIS_DATABASE_PASSWORD:-}" ]]; then
  POLARIS_DATABASE_PASSWORD="$(openssl rand -hex 24)"
  printf 'POLARIS_DATABASE_PASSWORD=%s\n' "${POLARIS_DATABASE_PASSWORD}" >>"${SETUP_ENV}"
fi

if [[ -z "${POLARIS_ROOT_CLIENT_SECRET:-}" ]]; then
  POLARIS_ROOT_CLIENT_SECRET="$(openssl rand -hex 32)"
  printf 'POLARIS_ROOT_CLIENT_SECRET=%s\n' "${POLARIS_ROOT_CLIENT_SECRET}" >>"${SETUP_ENV}"
fi

if [[ -z "${SUPERSET_DATABASE_PASSWORD:-}" ]]; then
  SUPERSET_DATABASE_PASSWORD="$(openssl rand -hex 24)"
  printf 'SUPERSET_DATABASE_PASSWORD=%s\n' "${SUPERSET_DATABASE_PASSWORD}" >>"${SETUP_ENV}"
fi

if [[ -z "${SUPERSET_SECRET_KEY:-}" ]]; then
  SUPERSET_SECRET_KEY="$(openssl rand -hex 32)"
  printf 'SUPERSET_SECRET_KEY=%s\n' "${SUPERSET_SECRET_KEY}" >>"${SETUP_ENV}"
fi

POLARIS_DATABASE_PASSWORD="${provided_database_password:-${POLARIS_DATABASE_PASSWORD}}"
POLARIS_ROOT_CLIENT_SECRET="${provided_root_client_secret:-${POLARIS_ROOT_CLIENT_SECRET}}"
SUPERSET_DATABASE_PASSWORD="${provided_superset_database_password:-${SUPERSET_DATABASE_PASSWORD}}"
SUPERSET_SECRET_KEY="${provided_superset_secret_key:-${SUPERSET_SECRET_KEY}}"
SUPERSET_ADMIN_PASSWORD="admin"

export POLARIS_DATABASE_PASSWORD
export POLARIS_ROOT_CLIENT_SECRET
export SUPERSET_DATABASE_PASSWORD
export SUPERSET_SECRET_KEY
export SUPERSET_ADMIN_PASSWORD
export TF_VAR_polaris_database_password="${POLARIS_DATABASE_PASSWORD}"
export TF_VAR_polaris_root_client_secret="${POLARIS_ROOT_CLIENT_SECRET}"
export TF_VAR_superset_database_password="${SUPERSET_DATABASE_PASSWORD}"
export TF_VAR_superset_secret_key="${SUPERSET_SECRET_KEY}"
export TF_VAR_superset_admin_password="${SUPERSET_ADMIN_PASSWORD}"

cleanup() {
  docker rm --force "${TERRAFORM_CONTAINER}" >/dev/null 2>&1 || true
}

gcloud_cmd() {
  docker run --rm \
    --volume "${GCLOUD_CONFIG}:/config" \
    --env CLOUDSDK_CONFIG=/config \
    "${GCLOUD_IMAGE}" \
    gcloud "$@"
}

trap cleanup EXIT
cleanup

printf '🟢 Terraform: Start container\n'
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

printf '🟢 Terraform: Provision GCP resources\n'
docker exec "${TERRAFORM_CONTAINER}" terraform init -input=false >/dev/null 2>&1
docker exec "${TERRAFORM_CONTAINER}" terraform apply \
  -json \
  -input=false \
  -auto-approve >"${TERRAFORM_APPLY_1_LOG}" 2>&1

printf '🟢 Polaris: Bootstrap realm and root credentials\n'
gcloud_cmd run jobs execute "${POLARIS_BOOTSTRAP_JOB}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --wait >/dev/null

printf '🟢 Docker: Build and push application images\n'
printf '   - GCloud: Request Artifact Registry access token\n'
access_token="$(gcloud_cmd auth print-access-token)"

printf '   - Docker: Authenticate to Artifact Registry\n'
printf '%s' "${access_token}" |
  docker login \
    --username oauth2accesstoken \
    --password-stdin \
    "${REGISTRY_HOST}" >/dev/null 2>&1

printf '   - Docker: Build and push ingestion image\n'
docker buildx build \
  --platform linux/amd64 \
  --provenance=false \
  --target runtime \
  --tag "${INGESTION_IMAGE}" \
  --push \
  "${PROJECT_DIR}/services/ingestion" >/dev/null 2>&1

printf '   - Docker: Build and push ML image\n'
docker buildx build \
  --platform linux/amd64 \
  --provenance=false \
  --target runtime \
  --tag "${ML_IMAGE}" \
  --push \
  "${PROJECT_DIR}/services/ml" >/dev/null 2>&1

printf '   - Docker: Build and push Superset image\n'
docker buildx build \
  --platform linux/amd64 \
  --provenance=false \
  --tag "${SUPERSET_IMAGE}" \
  --push \
  "${PROJECT_DIR}/services/superset" >/dev/null 2>&1

printf '🟢 Terraform: Update Cloud Run Job deployments\n'
docker exec "${TERRAFORM_CONTAINER}" terraform apply \
  -json \
  -input=false \
  -auto-approve \
  -var="ingestion_image=${INGESTION_IMAGE}" \
  -var="ml_image=${ML_IMAGE}" \
  -var="superset_image=${SUPERSET_IMAGE}" >"${TERRAFORM_APPLY_2_LOG}" 2>&1

printf '🟢 Polaris: Configure and verify Iceberg warehouse\n'
POLARIS_URL="$(
  gcloud_cmd run services describe "${POLARIS_SERVICE}" \
    --region="${REGION}" \
    --project="${PROJECT_ID}" \
    --format='value(status.url)'
)"
CLOUD_RUN_IDENTITY_TOKEN="$(gcloud_cmd auth print-identity-token)"
export POLARIS_URL
export CLOUD_RUN_IDENTITY_TOKEN

docker run --rm \
  --volume "${PROJECT_DIR}/services/polaris:/app:ro" \
  --workdir /app \
  --env POLARIS_URL \
  --env CLOUD_RUN_IDENTITY_TOKEN \
  --env POLARIS_ROOT_CLIENT_SECRET \
  --env POLARIS_WAREHOUSE="${POLARIS_WAREHOUSE}" \
  --env POLARIS_GCS_SERVICE_ACCOUNT="${POLARIS_SERVICE_ACCOUNT}" \
  python:3.12-slim \
  sh -c \
  'pip install --quiet --root-user-action=ignore --disable-pip-version-check --no-cache-dir -r requirements.txt && python configure.py'

printf '🟢 GCloud: Run end-to-end lakehouse pipeline\n'
gcloud_cmd workflows run lakehouse-pipeline \
  --location="${REGION}" \
  --project="${PROJECT_ID}" >/dev/null 2>&1

printf '🟢 Setup complete\n'
