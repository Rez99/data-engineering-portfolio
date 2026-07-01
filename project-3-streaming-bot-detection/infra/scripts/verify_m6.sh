#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
COMPOSE_FILES=(
  -f "${PROJECT_DIR}/infra/compose/kafka.yml"
  -f "${PROJECT_DIR}/infra/compose/kafka-ui.yml"
  -f "${PROJECT_DIR}/infra/compose/flink.yml"
  -f "${PROJECT_DIR}/infra/compose/postgres.yml"
  -f "${PROJECT_DIR}/infra/compose/grafana.yml"
)

cd "${PROJECT_DIR}"

container_id() {
  local service="$1"
  docker compose "${COMPOSE_FILES[@]}" ps --status running -q "${service}" 2>/dev/null
}

require_container() {
  local service="$1"
  if [[ -z "$(container_id "${service}")" ]]; then
    echo "M6 verification failed: ${service} is not running." >&2
    exit 1
  fi
}

require_http() {
  local name="$1"
  local url="$2"
  python3 - "${name}" "${url}" <<'PY'
import sys
from urllib.request import urlopen

name, url = sys.argv[1], sys.argv[2]
try:
    with urlopen(url, timeout=10) as response:
        if response.status >= 400:
            raise RuntimeError(f"HTTP {response.status}")
except Exception as exc:
    print(f"M6 verification failed: {name} is not reachable at {url}: {exc}", file=sys.stderr)
    sys.exit(1)
PY
}

flink_json() {
  local path="$1"
  python3 - "${path}" <<'PY'
import json
import sys
from urllib.request import urlopen

path = sys.argv[1]
with urlopen(f"http://localhost:8081{path}", timeout=10) as response:
    print(json.dumps(json.load(response)))
PY
}

rpk() {
  docker compose "${COMPOSE_FILES[@]}" exec -T redpanda rpk "$@" --brokers localhost:9092
}

require_container redpanda
require_container redpanda-console
require_container jobmanager
require_container taskmanager

require_http "Redpanda Console" "http://localhost:8080"
require_http "Flink Web UI" "http://localhost:8081/overview"

topic_list="$(rpk topic list)"
for topic in clickstream-raw clickstream-clean clickstream-dlq; do
  if ! printf '%s\n' "${topic_list}" | awk -v topic="${topic}" '$1 == topic { found = 1 } END { exit !found }'; then
    echo "M6 verification failed: missing Kafka topic ${topic}." >&2
    exit 1
  fi
done

group_report="$(rpk group describe m2-validation m3-analytics m4-operational m6-analytics-observer 2>/dev/null || true)"

job_report="$(flink_json /jobs/overview)"
checkpoint_report="$(python3 - <<'PY'
import json
from urllib.request import urlopen

jobs = json.load(urlopen("http://localhost:8081/jobs/overview", timeout=10))["jobs"]
running = [job for job in jobs if job["state"] == "RUNNING"]
for job in running:
    checkpoints = json.load(urlopen(f"http://localhost:8081/jobs/{job['jid']}/checkpoints", timeout=10))
    counts = checkpoints.get("counts", {})
    print(f"{job['name']}|{job['jid']}|{counts.get('completed', 0)}|{counts.get('failed', 0)}|{counts.get('restored', 0)}")
PY
)"

required_jobs_report="$(python3 - <<'PY'
import json
from urllib.request import urlopen

jobs = json.load(urlopen("http://localhost:8081/jobs/overview", timeout=10))["jobs"]
running_names = {job["name"] for job in jobs if job["state"] == "RUNNING"}
required = {
    "m2-clickstream-validation": "validation",
    "m6-analytics-observer": "analytical observer",
    "m4-operational-bot-scoring": "operational",
}
missing = [label for name, label in required.items() if name not in running_names]
if missing:
    print("missing|" + ", ".join(missing))
else:
    print("ok|validation, analytical observer, operational")
PY
)"

IFS='|' read -r required_state required_detail <<<"${required_jobs_report}"
if [[ "${required_state}" != "ok" ]]; then
  echo "M6 verification failed: missing running Flink job(s): ${required_detail}" >&2
  exit 1
fi

backpressure_report="$(python3 - <<'PY'
import json
from urllib.request import urlopen

jobs = json.load(urlopen("http://localhost:8081/jobs/overview", timeout=10))["jobs"]
for job in jobs:
    if job["state"] != "RUNNING":
        continue
    details = json.load(urlopen(f"http://localhost:8081/jobs/{job['jid']}", timeout=10))
    for vertex in details.get("vertices", []):
        metrics = json.load(urlopen(
            f"http://localhost:8081/jobs/{job['jid']}/vertices/{vertex['id']}/metrics"
            "?get=busyTimeMsPerSecond,backPressuredTimeMsPerSecond,numRecordsInPerSecond,numRecordsOutPerSecond",
            timeout=10,
        ))
        values = {metric["id"]: metric.get("value", "0") for metric in metrics}
        print(
            "{job}|{vertex}|busy_ms_per_s={busy}|backpressured_ms_per_s={backpressure}|records_in_per_s={records_in}|records_out_per_s={records_out}".format(
                job=job["name"],
                vertex=vertex["name"].replace("\n", " "),
                busy=values.get("busyTimeMsPerSecond", "0"),
                backpressure=values.get("backPressuredTimeMsPerSecond", "0"),
                records_in=values.get("numRecordsInPerSecond", "0"),
                records_out=values.get("numRecordsOutPerSecond", "0"),
            )
        )
PY
)"

dlq_description="$(rpk topic describe clickstream-dlq 2>/dev/null || true)"
dlq_high_watermark="$(rpk topic describe clickstream-dlq -p 2>/dev/null | awk '$1 == "0" { print $6 }')"
if [[ "${dlq_high_watermark:-0}" -gt 0 ]]; then
  dlq_sample="$(rpk topic consume clickstream-dlq --offset start --num 1 --format '%v\n' 2>/dev/null || true)"
else
  dlq_sample=""
fi
if [[ -z "${dlq_sample}" ]]; then
  dlq_sample="No DLQ records available in the current retained topic range."
fi

cat <<REPORT
M6 observability verification
redpanda_console_url=http://localhost:8080
flink_ui_url=http://localhost:8081

Kafka topics
${topic_list}

Consumer groups
${group_report}

Flink jobs
$(python3 - "${job_report}" <<'PY'
import json
import sys

jobs = json.loads(sys.argv[1])["jobs"]
for job in jobs:
    print(f"{job['name']}|{job['jid']}|{job['state']}|tasks={job['tasks']['running']}/{job['tasks']['total']}")
PY
)

Checkpoint status
${checkpoint_report}

Flink throughput/backpressure metrics
${backpressure_report}

DLQ topic
${dlq_description}

DLQ sample
${dlq_sample}
REPORT
