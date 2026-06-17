#!/usr/bin/env bash

set -euo pipefail

readonly PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly GCLOUD_IMAGE="gcr.io/google.com/cloudsdktool/google-cloud-cli:572.0.0-stable"
readonly GCLOUD_CONFIG="${PROJECT_DIR}/.credentials/gcloud"
readonly PROJECT_ID="rez-cloud-lakehouse"
readonly REGION="us-central1"
readonly PIPELINE_WORKFLOW="lakehouse-pipeline"

if [[ ! -d "${GCLOUD_CONFIG}" ]]; then
  printf 'Missing Google Cloud config directory: %s\n' "${GCLOUD_CONFIG}" >&2
  exit 1
fi

docker run --rm \
  --volume "${GCLOUD_CONFIG}:/config" \
  --env CLOUDSDK_CONFIG=/config \
  "${GCLOUD_IMAGE}" \
  gcloud workflows run "${PIPELINE_WORKFLOW}" \
  --location="${REGION}" \
  --project="${PROJECT_ID}"
