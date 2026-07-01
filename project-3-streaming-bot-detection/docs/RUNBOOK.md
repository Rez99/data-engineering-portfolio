# Streaming Bot Detection Runbook

This runbook is organized around the state you are in, not around project milestones.

## Table Of Contents

- [1. Provision Platform](#1-provision-platform)
- [2. Process Events](#2-process-events)
- [3. Reset Data](#3-reset-data)
- [4. Mental Model](#4-mental-model)

Start from the project root:

```bash
cd /Users/rezwanhoppe-islam/data-engineering-portfolio/project-3-streaming-bot-detection
open -a Docker
until docker info >/dev/null 2>&1; do
  sleep 2
done
```

## 1. Provision Platform

Use this when you do not care what currently exists. Some containers may be stopped, some jobs may already be running, or nothing may be running. Your goal is simply:

```text
Make the finished streaming platform available.
```

Run:

```bash
./infra/scripts/setup_all.sh
./infra/scripts/verify_all.sh
```

`setup_all.sh` is the desired-state command for the finished project. It starts or reuses:

```text
Redpanda/Kafka
Redpanda Console
Flink JobManager and TaskManager
PostgreSQL
Grafana
validation Flink job
operational bot-scoring Flink job
analytics observer Flink job
```

It also ensures the PostgreSQL schema exists and cancels the old M3 Parquet writer if it is accidentally running, because day-to-day demos should not append more rows to the historical Parquet dataset.

After `verify_all.sh` passes, you have these guarantees:

```text
replay.py compiles
Redpanda Console is reachable
Flink Web UI is reachable
Kafka topics exist: clickstream-raw, clickstream-clean, clickstream-dlq
validation, operational, and analytics observer Flink jobs are running
PostgreSQL operational tables exist
Grafana datasource and dashboard are available
M4 operational reference artifacts are present
Flink checkpoint/backpressure/throughput metrics are queryable
DLQ topic is inspectable
```

Successful output is intentionally scannable:

```text
Complete platform verification

Check                                          Status  Detail
-----                                          ------  ------
🟢 M1                                      ok      replay compiles; broker, console, and topics are available
🟢 M2                                      ok      validation job running; raw=1000, clean=1000, dlq=0
🟢 M3                                      ok      dataset materialized; partitions=1, parquet_files=2
🟢 M4                                      ok      artifacts, PostgreSQL tables, and operational job are available
🟢 M5                                      ok      dashboard=Streaming Bot Detection Live, datasource=Clickstream Postgres
🟢 M6                                      ok      topics, UIs, DLQ, metrics, running_jobs=3

verify_all=ok
```

If the historical M3 Parquet dataset exists, `verify_all.sh` validates it. If the dataset has not been materialized yet, the M3 row is green only while the M3 analytics writer is running.

Open the UIs:

```text
Redpanda Console: http://localhost:8080
Flink Web UI:     http://localhost:8081
Grafana:          http://localhost:3000
```

Grafana login:

```text
Username: admin
Password: admin
Dashboard: Streaming Bot Detection Live
```

Important prerequisite: the operational job depends on M4 reference artifacts:

```text
batch/artifacts/normalization.parquet
batch/artifacts/bot_config.json
data/flink/generated/flink_job_operational.sql.template
```

Those artifacts exist in the completed project state. If `verify_all.sh` says the operational job is missing, rebuild the operational artifacts from the existing M3 Parquet dataset:

```bash
./infra/scripts/setup_m4.sh
./infra/scripts/setup_all.sh
./infra/scripts/verify_all.sh
```

## 2. Process Events

Use this for normal day-to-day operation. Assume `verify_all.sh` already passes and you only want to push a batch through the live pipeline.

Run a small replay:

```bash
python3 streaming/replay.py \
  --start-row 100000 \
  --rows 500 \
  --speed 100x \
  --sink kafka \
  --no-sleep \
  --corrupt-probability 0.02 \
  --delay-probability 0.02 \
  --quiet \
  --progress-every 250
```

Then verify/observe:

```bash
./infra/scripts/verify_all.sh
```

What happens:

```text
replay.py publishes events to clickstream-raw
validation Flink job reads clickstream-raw
valid events are written to clickstream-clean
invalid events are written to clickstream-dlq
operational Flink job reads clickstream-clean
PostgreSQL receives session bot scores and stream metrics
Grafana visualizes PostgreSQL metrics
Redpanda Console and Flink UI show lag, topics, jobs, checkpoints, and backpressure
```

Where to look:

```text
Redpanda Console:
- topics: clickstream-raw, clickstream-clean, clickstream-dlq
- consumer groups and lag
- DLQ records

Flink Web UI:
- m2-clickstream-validation
- m6-analytics-observer
- m4-operational-bot-scoring
- operator graphs
- checkpoints
- throughput and backpressure

Grafana:
- Streaming Bot Detection Live
- session-level bot scores
- stream-level metrics
```

This mode appends to Kafka topics and updates PostgreSQL. That is fine for demos. It should not append to the historical M3 Parquet dataset because the M6 platform uses `m6-analytics-observer` instead of the old M3 Parquet writer.

## 3. Reset Data

Use this when the infrastructure is fine, but the data shown in Kafka/PostgreSQL/Grafana is stale, duplicated, or confusing. Your goal is:

```text
Keep the platform running, but clear processed/demo state so the next replay starts clean.
```

Run:

```bash
./infra/scripts/demo_reset.sh
```

Then replay a fresh batch:

```bash
python3 streaming/replay.py \
  --start-row 100000 \
  --rows 500 \
  --speed 100x \
  --sink kafka \
  --no-sleep \
  --corrupt-probability 0.02 \
  --delay-probability 0.02 \
  --quiet \
  --progress-every 250

./infra/scripts/verify_all.sh
```

`demo_reset.sh` clears processed/demo state:

| Cleared | Why |
|---|---|
| `clickstream-raw`, `clickstream-clean`, `clickstream-dlq` topic contents | Replaying events appends to Kafka. Clearing topics prevents old replay batches from mixing with the new demo batch. |
| `data/analytics/clickstream/` | Removes old M3 Parquet output so stale analytical files are not mistaken for current demo output. |
| PostgreSQL rows in `session_bot_scores` and `stream_bot_metrics` | Grafana reads these tables. Truncating them makes the dashboard reflect only the new replay. |
| `data/flink/checkpoints/` | Old checkpoints contain old source offsets/state. Clearing them avoids restoring from stale demo state. |

`demo_reset.sh` keeps durable inputs and infrastructure:

| Kept | Why |
|---|---|
| Docker infrastructure containers | The platform should remain available. |
| Source CSV in `data/source/` | This is the immutable input dataset, not processed output. |
| Flink connector jars in `data/flink/lib/` | These are dependencies and are expensive/noisy to redownload. |
| M4 reference artifacts in `batch/artifacts/` | These are historical normalization inputs used by the operational job. They are not per-demo output. |
| Grafana provisioning files | These define the dashboard and datasource. They are code/config, not processed data. |

One detail: `demo_reset.sh` keeps Flink infrastructure containers running, but it briefly cancels and resubmits the Flink job listeners. That is intentional. The script deletes and recreates the Kafka topics, so the Flink subscribers need to attach cleanly to the fresh topics afterward.

## 4. Mental Model

For normal use:

```bash
./infra/scripts/setup_all.sh
./infra/scripts/verify_all.sh
```

For a clean demo replay:

```bash
./infra/scripts/demo_reset.sh
```

For a full teardown of the complete platform:

```bash
./infra/scripts/reset_all.sh
```

For sending events:

```bash
python3 streaming/replay.py --start-row 100000 --rows 500 --speed 100x --sink kafka --no-sleep --corrupt-probability 0.02 --delay-probability 0.02 --quiet --progress-every 250
```
