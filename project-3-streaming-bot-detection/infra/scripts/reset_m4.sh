#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ARTIFACT_DIR="${PROJECT_DIR}/batch/artifacts"

rm -f "${ARTIFACT_DIR}/normalization.parquet"
rm -f "${ARTIFACT_DIR}/bot_config.json"

echo "Removed M4 generated artifacts from ${ARTIFACT_DIR}."
