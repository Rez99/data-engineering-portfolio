#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
COMPOSE_FILES=(
  -f "${PROJECT_DIR}/infra/compose/postgres.yml"
  -f "${PROJECT_DIR}/infra/compose/grafana.yml"
)

cd "${PROJECT_DIR}"

postgres_scalar() {
  local sql="$1"
  docker compose "${COMPOSE_FILES[@]}" exec -T postgres \
    psql -U clickstream -d clickstream -At -c "${sql}" | tr -d '\r'
}

if [[ -z "$(docker compose "${COMPOSE_FILES[@]}" ps -q grafana 2>/dev/null)" ]]; then
  echo "M5 verification failed: Grafana container is not running." >&2
  exit 1
fi

session_table_exists="$(postgres_scalar "SELECT to_regclass('public.session_bot_scores') IS NOT NULL;")"
metrics_table_exists="$(postgres_scalar "SELECT to_regclass('public.stream_bot_metrics') IS NOT NULL;")"

if [[ "${session_table_exists}" != "t" || "${metrics_table_exists}" != "t" ]]; then
  echo "M5 verification failed: expected M4 PostgreSQL tables are missing." >&2
  exit 1
fi

grafana_report="$(python3 - <<'PY'
import base64
import json
import sys
from urllib.request import Request, urlopen

auth = base64.b64encode(b"admin:admin").decode("ascii")

def get_json(path):
    request = Request(f"http://localhost:3000{path}")
    request.add_header("Authorization", f"Basic {auth}")
    with urlopen(request, timeout=10) as response:
        return json.load(response)

try:
    health = get_json("/api/health")
    datasource = get_json("/api/datasources/uid/clickstream-postgres")
    dashboard = get_json("/api/dashboards/uid/streaming-bot-detection-live")
except Exception as exc:
    print(f"error|{exc}")
    sys.exit(0)

print(
    "ok|{datasource}|{database}|{dashboard}|{version}".format(
        datasource=datasource.get("name", ""),
        database=datasource.get("jsonData", {}).get("database", ""),
        dashboard=dashboard.get("dashboard", {}).get("title", ""),
        version=health.get("version", ""),
    )
)
PY
)"

IFS='|' read -r grafana_state grafana_datasource grafana_database grafana_dashboard grafana_version <<<"${grafana_report}"
if [[ "${grafana_state}" != "ok" ]]; then
  echo "M5 verification failed: Grafana API check failed: ${grafana_datasource}" >&2
  exit 1
fi

session_rows="$(postgres_scalar "SELECT COUNT(*) FROM session_bot_scores;")"
metric_rows="$(postgres_scalar "SELECT COUNT(*) FROM stream_bot_metrics;")"
latest_bot_rate="$(postgres_scalar "SELECT COALESCE((SELECT bot_rate FROM stream_bot_metrics ORDER BY window_end DESC LIMIT 1), 0.0);")"

cat <<REPORT
M5 Grafana dashboard
grafana_url=http://localhost:3000
grafana_version=${grafana_version}
datasource_name=${grafana_datasource}
datasource_database=${grafana_database}
dashboard_title=${grafana_dashboard}
postgres_session_rows=${session_rows}
postgres_metric_rows=${metric_rows}
latest_bot_rate=${latest_bot_rate}
REPORT
