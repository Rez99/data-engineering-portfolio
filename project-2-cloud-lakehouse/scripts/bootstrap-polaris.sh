#!/usr/bin/env bash

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
readonly GCLOUD_IMAGE="gcr.io/google.com/cloudsdktool/google-cloud-cli:572.0.0-stable"
readonly GCLOUD_CONFIG="${PROJECT_DIR}/.credentials/gcloud"
readonly PROJECT_ID="${PROJECT_ID:-rez-cloud-lakehouse}"
readonly REGION="${REGION:-us-central1}"
readonly POLARIS_BOOTSTRAP_JOB="${POLARIS_BOOTSTRAP_JOB:-polaris-bootstrap}"

if [[ ! -d "${GCLOUD_CONFIG}" ]]; then
  printf 'Missing Google Cloud config directory: %s\n' "${GCLOUD_CONFIG}" >&2
  exit 1
fi

docker run --rm \
  --volume "${GCLOUD_CONFIG}:/config" \
  --env CLOUDSDK_CONFIG=/config \
  "${GCLOUD_IMAGE}" \
  gcloud run jobs execute "${POLARIS_BOOTSTRAP_JOB}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --wait
