#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PARQUET_DIR="${PROJECT_DIR}/data/analytics/clickstream"
ARTIFACT_DIR="${PROJECT_DIR}/batch/artifacts"
DUCKDB_COMPOSE_FILES=(-f "${PROJECT_DIR}/infra/compose/duckdb.yml")
STREAMING_COMPOSE_FILES=(
  -f "${PROJECT_DIR}/infra/compose/kafka.yml"
  -f "${PROJECT_DIR}/infra/compose/kafka-ui.yml"
  -f "${PROJECT_DIR}/infra/compose/flink.yml"
  -f "${PROJECT_DIR}/infra/compose/postgres.yml"
)

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

if [[ ! -f "${PROJECT_DIR}/data/flink/generated/operational-bot-scoring.jar" ]]; then
  echo "M4 verification failed: missing ${PROJECT_DIR}/data/flink/generated/operational-bot-scoring.jar" >&2
  echo "Run ./infra/scripts/build_operational_job.sh first." >&2
  exit 1
fi

cd "${PROJECT_DIR}"

duckdb_scalar() {
  local sql="$1"
  docker compose "${DUCKDB_COMPOSE_FILES[@]}" run --rm -T duckdb \
    -csv -noheader -c "${sql}" | tr -d '\r'
}

duckdb_csv() {
  local sql="$1"
  docker compose "${DUCKDB_COMPOSE_FILES[@]}" run --rm -T duckdb \
    -csv -noheader -c "${sql}" | tr -d '\r'
}

postgres_scalar() {
  local sql="$1"
  docker compose "${STREAMING_COMPOSE_FILES[@]}" exec -T postgres \
    psql -U clickstream -d clickstream -At -c "${sql}" | tr -d '\r'
}

flink_job_summary() {
  python3 - <<'PY'
import json
import sys
from urllib.request import urlopen

try:
    jobs = json.load(urlopen("http://localhost:8081/jobs/overview", timeout=5))["jobs"]
except Exception as exc:
    print(f"error|0|0|{exc}")
    sys.exit(0)

running = [job for job in jobs if job["name"] == "m4-operational-bot-scoring" and job["state"] == "RUNNING"]
if not running:
    print("missing|0|0|m4-operational-bot-scoring is not RUNNING")
    sys.exit(0)

job_id = running[0]["jid"]
try:
    checkpoints = json.load(urlopen(f"http://localhost:8081/jobs/{job_id}/checkpoints", timeout=5))
except Exception as exc:
    print(f"running|0|0|{exc}")
    sys.exit(0)

counts = checkpoints.get("counts", {})
print(
    "running|{completed}|{restored}|{job_id}".format(
        completed=counts.get("completed", 0),
        restored=counts.get("restored", 0),
        job_id=job_id,
    )
)
PY
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

if [[ -n "$(docker compose "${STREAMING_COMPOSE_FILES[@]}" ps -q postgres 2>/dev/null)" ]]; then
  session_table_exists="$(postgres_scalar "SELECT to_regclass('public.session_bot_scores') IS NOT NULL;")"
  metrics_table_exists="$(postgres_scalar "SELECT to_regclass('public.stream_bot_metrics') IS NOT NULL;")"
  session_rows="$(postgres_scalar "SELECT COUNT(*) FROM session_bot_scores;")"
  metric_rows="$(postgres_scalar "SELECT COUNT(*) FROM stream_bot_metrics;")"
else
  session_table_exists="false"
  metrics_table_exists="false"
  session_rows="0"
  metric_rows="0"
fi

if [[ "${session_table_exists}" != "t" || "${metrics_table_exists}" != "t" ]]; then
  echo "M4 verification failed: expected PostgreSQL operational tables are missing." >&2
  exit 1
fi

IFS='|' read -r flink_state completed_checkpoints restored_checkpoints flink_detail < <(flink_job_summary)
if [[ "${flink_state}" != "running" ]]; then
  echo "M4 verification failed: ${flink_detail}" >&2
  exit 1
fi

if [[ "${completed_checkpoints}" -lt 1 ]]; then
  echo "M4 verification failed: operational Flink job has no completed checkpoints yet." >&2
  echo "Wait for the Flink UI to show at least one completed checkpoint, then rerun this script." >&2
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
postgres_session_rows=${session_rows}
postgres_metric_rows=${metric_rows}
flink_operational_job_id=${flink_detail}
flink_completed_checkpoints=${completed_checkpoints}
flink_restored_checkpoints=${restored_checkpoints}
REPORT
