#!/usr/bin/env bash

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
readonly GCLOUD_CONFIG_DIR="${PROJECT_DIR}/.credentials/gcloud"
readonly GCLOUD_IMAGE="gcr.io/google.com/cloudsdktool/google-cloud-cli:572.0.0-stable"

mkdir -p "${GCLOUD_CONFIG_DIR}"

docker_args=(
  run
  --rm
  --volume "${GCLOUD_CONFIG_DIR}:/config"
  --env CLOUDSDK_CONFIG=/config
)

if [[ -t 0 && -t 1 ]]; then
  docker_args+=(--interactive --tty)
fi

docker "${docker_args[@]}" "${GCLOUD_IMAGE}" gcloud "$@"
