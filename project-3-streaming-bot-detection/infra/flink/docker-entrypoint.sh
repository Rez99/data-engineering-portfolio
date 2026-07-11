#!/usr/bin/env bash
set -euo pipefail

append_flink_properties() {
  if [[ -n "${FLINK_PROPERTIES:-}" ]]; then
    printf '\n%s\n' "${FLINK_PROPERTIES}" >> "${FLINK_HOME}/conf/flink-conf.yaml"
  fi
}

append_flink_properties

case "${1:-}" in
  jobmanager)
    shift
    exec "${FLINK_HOME}/bin/jobmanager.sh" start-foreground "$@"
    ;;
  taskmanager)
    shift
    exec "${FLINK_HOME}/bin/taskmanager.sh" start-foreground "$@"
    ;;
  *)
    exec "$@"
    ;;
esac
