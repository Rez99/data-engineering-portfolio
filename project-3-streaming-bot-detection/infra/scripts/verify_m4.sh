#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PARQUET_DIR="${PROJECT_DIR}/data/analytics/clickstream"
ARTIFACT_DIR="${PROJECT_DIR}/batch/artifacts"
COMPOSE_FILES=(-f "${PROJECT_DIR}/infra/compose/duckdb.yml")

if [[ ! -f "${ARTIFACT_DIR}/normalization.parquet" ]]; then
  echo "M4 verification failed: missing ${ARTIFACT_DIR}/normalization.parquet" >&2
  echo "Run ./infra/scripts/setup_m4.sh first." >&2
  exit 1
fi

if [[ ! -f "${ARTIFACT_DIR}/bot_config.json" ]]; then
  echo "M4 verification failed: missing ${ARTIFACT_DIR}/bot_config.json" >&2
  echo "Run ./infra/scripts/setup_m4.sh first." >&2
  exit 1
fi

cd "${PROJECT_DIR}"

duckdb_scalar() {
  local sql="$1"
  docker compose "${COMPOSE_FILES[@]}" run --rm -T duckdb \
    -csv -noheader -c "${sql}" | tr -d '\r'
}

duckdb_csv() {
  local sql="$1"
  docker compose "${COMPOSE_FILES[@]}" run --rm -T duckdb \
    -csv -noheader -c "${sql}" | tr -d '\r'
}

source_rows="$(duckdb_scalar "
  SELECT COUNT(*)
  FROM read_parquet('/work/data/analytics/clickstream/event_date=*/part-*', hive_partitioning = true);
")"
normalization_rows="$(duckdb_scalar "
  SELECT COUNT(*)
  FROM read_parquet('/work/batch/artifacts/normalization.parquet');
")"
normalization_schema_csv="$(duckdb_csv "
  DESCRIBE SELECT * FROM read_parquet('/work/batch/artifacts/normalization.parquet');
")"
normalization_columns=0
for required_column in percentile percentile_fraction mean_click_interval_ms min_click_interval_ms max_click_interval_ms sd_click_interval_ms; do
  if printf '%s\n' "${normalization_schema_csv}" | awk -F, -v column="${required_column}" '$1 == column { found = 1 } END { exit !found }'; then
    normalization_columns=$((normalization_columns + 1))
  fi
done
partition_count="$(find "${PARQUET_DIR}" -type d -name 'event_date=*' | wc -l | tr -d '[:space:]')"
parquet_file_count="$(find "${PARQUET_DIR}" -type f -name 'part-*' ! -name '*.inprogress*' | wc -l | tr -d '[:space:]')"

if [[ "${source_rows}" -eq 0 ]]; then
  echo "M4 verification failed: DuckDB found zero source rows." >&2
  exit 1
fi

if [[ "${normalization_rows}" -ne 101 ]]; then
  echo "M4 verification failed: expected 101 normalization rows, found ${normalization_rows}." >&2
  exit 1
fi

if [[ "${normalization_columns}" -ne 6 ]]; then
  echo "M4 verification failed: normalization.parquet schema is incomplete." >&2
  exit 1
fi

if ! grep -q '"session_inactivity_timeout_ms"' "${ARTIFACT_DIR}/bot_config.json"; then
  echo "M4 verification failed: bot_config.json is missing session_inactivity_timeout_ms." >&2
  exit 1
fi

cat <<REPORT
M4 DuckDB reference artifacts
source_path=${PARQUET_DIR}
source_rows=${source_rows}
event_date_partitions=${partition_count}
parquet_part_files=${parquet_file_count}
normalization_path=${ARTIFACT_DIR}/normalization.parquet
normalization_rows=${normalization_rows}
bot_config_path=${ARTIFACT_DIR}/bot_config.json
REPORT
