#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
JAVA_DIR="${PROJECT_DIR}/streaming/java"
GENERATED_DIR="${PROJECT_DIR}/data/flink/generated"
MAVEN_IMAGE="${MAVEN_IMAGE:-maven:3.9.9-eclipse-temurin-17}"

mkdir -p "${GENERATED_DIR}"

docker run --rm \
  -v "${JAVA_DIR}:/work" \
  -v "${GENERATED_DIR}/m2:/root/.m2" \
  -w /work \
  "${MAVEN_IMAGE}" \
  mvn -q -DskipTests package

cp "${JAVA_DIR}/target/operational-bot-scoring-1.0.0.jar" \
  "${GENERATED_DIR}/operational-bot-scoring.jar"

cat <<REPORT
Operational Flink job built:
  data/flink/generated/operational-bot-scoring.jar
REPORT
