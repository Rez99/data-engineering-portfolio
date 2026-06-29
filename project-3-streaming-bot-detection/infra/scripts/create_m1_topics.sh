#!/usr/bin/env bash
set -euo pipefail

BROKERS="${BROKERS:-redpanda:9092}"

create_topic_if_missing() {
  local topic="$1"
  local partitions="$2"
  local retention_ms="$3"

  if rpk topic describe "${topic}" --brokers "${BROKERS}" >/dev/null 2>&1; then
    echo "Topic already exists: ${topic}"
    return
  fi

  rpk topic create "${topic}" \
    --brokers "${BROKERS}" \
    --partitions "${partitions}" \
    --replicas 1 \
    --topic-config "retention.ms=${retention_ms}" \
    --topic-config "cleanup.policy=delete"
}

create_topic_if_missing clickstream-raw 3 604800000
create_topic_if_missing clickstream-clean 3 604800000
create_topic_if_missing clickstream-dlq 1 1209600000

rpk topic list --brokers "${BROKERS}"
