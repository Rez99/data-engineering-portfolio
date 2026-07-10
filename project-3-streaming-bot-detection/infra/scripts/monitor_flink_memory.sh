#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
INTERVAL_SECONDS="${1:-5}"
OUTPUT_DIR="${2:-${PROJECT_DIR}/datasets/monitoring/flink-memory-$(date -u +%Y%m%dT%H%M%SZ)}"
TASKMANAGER_CONTAINER="p3-flink-taskmanager"
FLINK_REST_URL="${FLINK_REST_URL:-http://localhost:8081}"

mkdir -p "${OUTPUT_DIR}"

MEMORY_FILE="${OUTPUT_DIR}/memory.csv"
JSTAT_FILE="${OUTPUT_DIR}/jstat-gc.log"
NMT_FILE="${OUTPUT_DIR}/nmt-summary.log"
FLINK_FILE="${OUTPUT_DIR}/flink-state.jsonl"
GAUGE_FILE="${OUTPUT_DIR}/session-gauges.jsonl"
TASKMANAGER_METRICS_FILE="${OUTPUT_DIR}/taskmanager-metrics.jsonl"

echo "timestamp_utc,cgroup_bytes,anon_bytes,file_bytes,anon_thp_bytes,rss_kb,rss_anon_kb,threads,restart_count" > "${MEMORY_FILE}"
: > "${JSTAT_FILE}"
: > "${NMT_FILE}"
: > "${FLINK_FILE}"
: > "${GAUGE_FILE}"
: > "${TASKMANAGER_METRICS_FILE}"

last_checkpoint=""
sample_number=0

echo "Writing Flink memory diagnostics to ${OUTPUT_DIR}"
echo "Sampling every ${INTERVAL_SECONDS}s; stop with Ctrl-C."

while true; do
  timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  sample_number=$((sample_number + 1))

  memory_current="$(docker exec "${TASKMANAGER_CONTAINER}" cat /sys/fs/cgroup/memory.current)"
  memory_stat="$(docker exec "${TASKMANAGER_CONTAINER}" cat /sys/fs/cgroup/memory.stat)"
  process_status="$(docker exec "${TASKMANAGER_CONTAINER}" cat /proc/1/status)"
  restart_count="$(docker inspect -f '{{.RestartCount}}' "${TASKMANAGER_CONTAINER}")"

  anon="$(awk '$1 == "anon" {print $2}' <<<"${memory_stat}")"
  file="$(awk '$1 == "file" {print $2}' <<<"${memory_stat}")"
  anon_thp="$(awk '$1 == "anon_thp" {print $2}' <<<"${memory_stat}")"
  rss_kb="$(awk '$1 == "VmRSS:" {print $2}' <<<"${process_status}")"
  rss_anon_kb="$(awk '$1 == "RssAnon:" {print $2}' <<<"${process_status}")"
  threads="$(awk '$1 == "Threads:" {print $2}' <<<"${process_status}")"

  echo "${timestamp},${memory_current},${anon},${file},${anon_thp},${rss_kb},${rss_anon_kb},${threads},${restart_count}" >> "${MEMORY_FILE}"

  {
    echo "timestamp_utc=${timestamp}"
    docker exec "${TASKMANAGER_CONTAINER}" jstat -gc 1
  } >> "${JSTAT_FILE}" 2>&1 || true

  if (( sample_number == 1 || sample_number % 6 == 0 )); then
    {
      echo "===== ${timestamp} ====="
      docker exec "${TASKMANAGER_CONTAINER}" jcmd 1 VM.native_memory summary scale=MB
    } >> "${NMT_FILE}" 2>&1 || true
  fi

  jobs_json="$(curl -fsS "${FLINK_REST_URL}/jobs/overview" || true)"
  taskmanagers_json="$(curl -fsS "${FLINK_REST_URL}/taskmanagers" || true)"
  taskmanager_id="$(jq -r '.taskmanagers[0].id // empty' <<<"${taskmanagers_json}")"
  if [[ -n "${taskmanager_id}" ]]; then
    encoded_taskmanager_id="${taskmanager_id/:/%3A}"
    taskmanager_metrics="$(curl -fsS -G \
      --data-urlencode 'get=Status.JVM.Memory.Heap.Used,Status.JVM.Memory.Heap.Committed,Status.JVM.Memory.Heap.Max,Status.JVM.Memory.NonHeap.Used,Status.JVM.Memory.Metaspace.Used,Status.JVM.Memory.Direct.MemoryUsed,Status.JVM.Memory.Direct.TotalCapacity,Status.JVM.Memory.Mapped.MemoryUsed,Status.Flink.Memory.Managed.Used,Status.Flink.Memory.Managed.Total,Status.Shuffle.Netty.UsedMemory,Status.Shuffle.Netty.TotalMemory,Status.JVM.GarbageCollector.G1_Young_Generation.Count,Status.JVM.GarbageCollector.G1_Old_Generation.Count' \
      "${FLINK_REST_URL}/taskmanagers/${encoded_taskmanager_id}/metrics" || true)"
    jq -cn \
      --arg timestamp "${timestamp}" \
      --arg taskmanager_id "${taskmanager_id}" \
      --argjson metrics "${taskmanager_metrics:-[]}" \
      '{timestamp_utc:$timestamp,taskmanager_id:$taskmanager_id,metrics:$metrics}' \
      >> "${TASKMANAGER_METRICS_FILE}"
  fi

  operational_job_id="$(jq -r '.jobs[] | select(.name == "m4-operational-bot-scoring" and .state == "RUNNING") | .jid' <<<"${jobs_json}" | head -n 1)"

  if [[ -n "${operational_job_id}" ]]; then
    checkpoints_json="$(curl -fsS "${FLINK_REST_URL}/jobs/${operational_job_id}/checkpoints" || true)"
    checkpoint_id="$(jq -r '.latest.completed.id // empty' <<<"${checkpoints_json}")"

    if [[ -n "${checkpoint_id}" && "${operational_job_id}:${checkpoint_id}" != "${last_checkpoint}" ]]; then
      checkpoint_details="$(curl -fsS "${FLINK_REST_URL}/jobs/${operational_job_id}/checkpoints/details/${checkpoint_id}" || true)"
      jq -c --arg timestamp "${timestamp}" --arg job_id "${operational_job_id}" \
        '{timestamp_utc:$timestamp,job_id:$job_id,checkpoint:.}' \
        <<<"${checkpoint_details}" >> "${FLINK_FILE}"
      last_checkpoint="${operational_job_id}:${checkpoint_id}"
    fi

    job_details="$(curl -fsS "${FLINK_REST_URL}/jobs/${operational_job_id}" || true)"
    session_vertex="$(jq -r '.vertices[] | select(.name | startswith("event-time-session-scoring")) | .id' <<<"${job_details}" | head -n 1)"

    if [[ -n "${session_vertex}" ]]; then
      for subtask in 0 1 2; do
        metrics_url="${FLINK_REST_URL}/jobs/${operational_job_id}/vertices/${session_vertex}/subtasks/${subtask}/metrics"
        metric_ids="$(curl -fsS "${metrics_url}" | jq -r '.[].id | select(endswith("activeSessions") or endswith("retainedEventTimestamps"))' || true)"
        while IFS= read -r metric_id; do
          [[ -z "${metric_id}" ]] && continue
          metric_value="$(curl -fsS -G --data-urlencode "get=${metric_id}" "${metrics_url}" | jq -r '.[0].value // empty' || true)"
          jq -cn \
            --arg timestamp "${timestamp}" \
            --arg job_id "${operational_job_id}" \
            --arg vertex_id "${session_vertex}" \
            --argjson subtask "${subtask}" \
            --arg metric "${metric_id}" \
            --arg value "${metric_value}" \
            '{timestamp_utc:$timestamp,job_id:$job_id,vertex_id:$vertex_id,subtask:$subtask,metric:$metric,value:$value}' \
            >> "${GAUGE_FILE}"
        done <<< "${metric_ids}"
      done
    fi
  fi

  sleep "${INTERVAL_SECONDS}"
done
