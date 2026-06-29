#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PARQUET_DIR="${PROJECT_DIR}/data/analytics/clickstream"

if [[ ! -d "${PARQUET_DIR}" ]]; then
  echo "M3 analytical dataset not found: ${PARQUET_DIR}" >&2
  exit 1
fi

partition_count="$(find "${PARQUET_DIR}" -type d -name 'event_date=*' | wc -l | tr -d '[:space:]')"
parquet_file_count="$(find "${PARQUET_DIR}" -type f -name 'part-*' ! -name '*.inprogress*' | wc -l | tr -d '[:space:]')"
success_file_count="$(find "${PARQUET_DIR}" -type f -name '_SUCCESS' | wc -l | tr -d '[:space:]')"

cat <<REPORT
M3 analytical dataset
path=${PARQUET_DIR}
event_date_partitions=${partition_count}
parquet_part_files=${parquet_file_count}
success_files=${success_file_count}
REPORT

if [[ "${partition_count}" -eq 0 || "${parquet_file_count}" -eq 0 ]]; then
  echo "M3 verification failed: expected partitioned Parquet output." >&2
  exit 1
fi
