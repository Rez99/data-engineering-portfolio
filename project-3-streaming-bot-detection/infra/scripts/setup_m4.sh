#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ARTIFACT_DIR="${PROJECT_DIR}/batch/artifacts"
PARQUET_DIR="${PROJECT_DIR}/data/analytics/clickstream"
PARQUET_GLOB="/work/data/analytics/clickstream/event_date=*/part-*"
NORMALIZATION_SQL_PATH="${PROJECT_DIR}/batch/normalization.sql"
COMPOSE_FILES=(-f "${PROJECT_DIR}/infra/compose/duckdb.yml")
EVENTS_VIEW_SQL="CREATE OR REPLACE TEMP VIEW events AS SELECT * FROM read_parquet('${PARQUET_GLOB}', hive_partitioning = true);"

cd "${PROJECT_DIR}"

if [[ ! -d "${PARQUET_DIR}" ]]; then
  echo "M4 setup failed: M3 Parquet dataset not found at ${PARQUET_DIR}" >&2
  exit 1
fi

partition_count="$(find "${PARQUET_DIR}" -type d -name 'event_date=*' | wc -l | tr -d '[:space:]')"
parquet_file_count="$(find "${PARQUET_DIR}" -type f -name 'part-*' ! -name '*.inprogress*' | wc -l | tr -d '[:space:]')"

if [[ "${partition_count}" -eq 0 || "${parquet_file_count}" -eq 0 ]]; then
  echo "M4 setup failed: expected completed partitioned Parquet files in ${PARQUET_DIR}" >&2
  exit 1
fi

mkdir -p "${ARTIFACT_DIR}"

duckdb_scalar() {
  local sql="$1"
  docker compose "${COMPOSE_FILES[@]}" run --rm -T duckdb \
    -csv -noheader -c "${EVENTS_VIEW_SQL} ${sql}" | tr -d '\r'
}

duckdb_csv() {
  local sql="$1"
  docker compose "${COMPOSE_FILES[@]}" run --rm -T duckdb \
    -csv -noheader -c "${EVENTS_VIEW_SQL} ${sql}" | tr -d '\r'
}

duckdb_exec() {
  local sql="$1"
  docker compose "${COMPOSE_FILES[@]}" run --rm -T duckdb \
    -c "${EVENTS_VIEW_SQL} ${sql}"
}

source_rows="$(duckdb_scalar "SELECT COUNT(*) FROM events;")"
schema_csv="$(duckdb_csv "DESCRIBE SELECT * FROM events;")"
required_schema_columns=0
for required_column in event_time event_type product_id user_id user_session; do
  if printf '%s\n' "${schema_csv}" | awk -F, -v column="${required_column}" '$1 == column { found = 1 } END { exit !found }'; then
    required_schema_columns=$((required_schema_columns + 1))
  fi
done

if [[ "${source_rows}" -eq 0 ]]; then
  echo "M4 setup failed: DuckDB queried the M3 dataset but found zero rows." >&2
  exit 1
fi

if [[ "${required_schema_columns}" -ne 5 ]]; then
  echo "M4 setup failed: M3 dataset schema is missing one or more required columns." >&2
  exit 1
fi

normalization_query="$(<"${NORMALIZATION_SQL_PATH}")"
duckdb_exec "
  COPY (${normalization_query})
  TO '/work/batch/artifacts/normalization.parquet'
  (FORMAT PARQUET);
"

session_count="$(duckdb_scalar "
  WITH ordered_events AS (
    SELECT user_session, CAST(event_time AS TIMESTAMP) AS event_time
    FROM events
    WHERE user_session IS NOT NULL AND event_time IS NOT NULL
  ),
  click_intervals AS (
    SELECT
      user_session,
      EXTRACT(EPOCH FROM (
        event_time - LAG(event_time) OVER (
          PARTITION BY user_session
          ORDER BY event_time
        )
      )) * 1000 AS click_interval_ms
    FROM ordered_events
  ),
  session_metrics AS (
    SELECT user_session, MAX(click_interval_ms) AS max_click_interval_ms
    FROM click_intervals
    WHERE click_interval_ms IS NOT NULL AND click_interval_ms >= 0
    GROUP BY user_session
  )
  SELECT COUNT(*) FROM session_metrics;
")"

session_inactivity_timeout_ms="$(duckdb_scalar "
  WITH ordered_events AS (
    SELECT user_session, CAST(event_time AS TIMESTAMP) AS event_time
    FROM events
    WHERE user_session IS NOT NULL AND event_time IS NOT NULL
  ),
  click_intervals AS (
    SELECT
      user_session,
      EXTRACT(EPOCH FROM (
        event_time - LAG(event_time) OVER (
          PARTITION BY user_session
          ORDER BY event_time
        )
      )) * 1000 AS click_interval_ms
    FROM ordered_events
  ),
  session_metrics AS (
    SELECT user_session, MAX(click_interval_ms) AS max_click_interval_ms
    FROM click_intervals
    WHERE click_interval_ms IS NOT NULL AND click_interval_ms >= 0
    GROUP BY user_session
  )
  SELECT CAST(ceil(quantile_cont(max_click_interval_ms, 0.99)) AS BIGINT)
  FROM session_metrics;
")"

generated_at_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

cat > "${ARTIFACT_DIR}/bot_config.json" <<CONFIG
{
  "bot_score_threshold": 0.7,
  "generated_at_utc": "${generated_at_utc}",
  "normalization_artifact": "batch/artifacts/normalization.parquet",
  "session_count": ${session_count},
  "session_inactivity_timeout_ms": ${session_inactivity_timeout_ms},
  "session_inactivity_timeout_source": {
    "metric": "max_click_interval_ms",
    "percentile": 99
  },
  "source_dataset": "data/analytics/clickstream",
  "source_event_date_partitions": ${partition_count},
  "source_parquet_files": ${parquet_file_count},
  "source_row_count": ${source_rows}
}
CONFIG

"${SCRIPT_DIR}/verify_m4.sh"

echo
echo "M4.1/M4.2 artifacts are ready."
echo "DuckDB is available through Docker Compose: infra/compose/duckdb.yml"
echo "Artifacts: batch/artifacts/normalization.parquet and batch/artifacts/bot_config.json"
