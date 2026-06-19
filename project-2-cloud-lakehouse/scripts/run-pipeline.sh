#!/usr/bin/env bash

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
readonly GCLOUD_IMAGE="gcr.io/google.com/cloudsdktool/google-cloud-cli:572.0.0-stable"
readonly GCLOUD_CONFIG="${PROJECT_DIR}/.credentials/gcloud"
readonly PROJECT_ID="rez-cloud-lakehouse"
readonly REGION="us-central1"
readonly PIPELINE_WORKFLOW="pipeline"

if [[ ! -d "${GCLOUD_CONFIG}" ]]; then
  printf 'Missing Google Cloud config directory: %s\n' "${GCLOUD_CONFIG}" >&2
  exit 1
fi

docker_args=(
  run
  --rm
  --volume "${GCLOUD_CONFIG}:/config" \
  --volume "${PROJECT_DIR}/deployment/workflows/run_pipeline.py:/app/run_pipeline.py:ro" \
  --env CLOUDSDK_CONFIG=/config \
  --env PYTHONUNBUFFERED=1 \
  --env PROJECT_ID="${PROJECT_ID}" \
  --env REGION="${REGION}" \
  --env PIPELINE_WORKFLOW="${PIPELINE_WORKFLOW}" \
  "${GCLOUD_IMAGE}"
  python3
  /app/run_pipeline.py
)

docker "${docker_args[@]}"
