#!/usr/bin/env bash

set -euo pipefail

readonly PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly TERRAFORM_CONTAINER="lakehouse-terraform"
readonly TERRAFORM_IMAGE="hashicorp/terraform:1.15.6"
readonly GCLOUD_IMAGE="gcr.io/google.com/cloudsdktool/google-cloud-cli:572.0.0-stable"
readonly GCLOUD_CONFIG="${PROJECT_DIR}/.credentials/gcloud"
readonly ADC_FILE="${GCLOUD_CONFIG}/application_default_credentials.json"
readonly PROJECT_ID="rez-cloud-lakehouse"
readonly REGION="us-central1"
readonly REGISTRY_HOST="${REGION}-docker.pkg.dev"
readonly INGESTION_IMAGE="${REGISTRY_HOST}/${PROJECT_ID}/pipeline/ingestion:dev-amd64"

if [[ ! -f "${ADC_FILE}" ]]; then
  printf 'Missing Google Cloud credentials: %s\n' "${ADC_FILE}" >&2
  exit 1
fi

cleanup() {
  docker rm --force "${TERRAFORM_CONTAINER}" >/dev/null 2>&1 || true
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
  --workdir /workspace/terraform \
  "${TERRAFORM_IMAGE}" \
  -c "sleep infinity" >/dev/null

printf '🟢 Terraform: Provision GCP resources\n'
docker exec "${TERRAFORM_CONTAINER}" terraform init -input=false >/dev/null 2>&1
docker exec "${TERRAFORM_CONTAINER}" terraform apply -input=false -auto-approve >/dev/null 2>&1

printf '🟢 Docker: Build and push ingestion image\n'
printf '   - GCloud: Request Artifact Registry access token\n'
access_token="$(
  docker run --rm \
    --volume "${GCLOUD_CONFIG}:/config" \
    --env CLOUDSDK_CONFIG=/config \
    "${GCLOUD_IMAGE}" \
    gcloud auth print-access-token
)" 

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

printf '🟢 Terraform: Update Cloud Run Job deployment\n'
docker exec "${TERRAFORM_CONTAINER}" terraform apply \
  -input=false \
  -auto-approve \
  -var="ingestion_image=${INGESTION_IMAGE}"   >/dev/null 2>&1

printf '🟢 GCloud: Execute extraction workflow\n'
docker run --rm \
  --volume "${GCLOUD_CONFIG}:/config" \
  --env CLOUDSDK_CONFIG=/config \
  "${GCLOUD_IMAGE}" \
  gcloud workflows run lakehouse-extract \
  --location="${REGION}" \
  --project="${PROJECT_ID}" >/dev/null 2>&1

printf '🟢 Setup complete\n'
