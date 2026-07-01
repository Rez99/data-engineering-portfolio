# Project 3: Streaming Bot Detection

This project builds a real-time clickstream pipeline for bot detection. It replays the October 2019 e-commerce clickstream dataset into Kafka, validates raw events into a clean stream, persists analytical history to Parquet, and computes live session-level bot metrics for PostgreSQL and dashboarding.

The current implementation has completed Milestone 1 and implements the Milestone 2 validation layer: the replay application publishes JSON clickstream records to the raw `clickstream-raw` topic, and a Flink SQL validation job routes valid records to `clickstream-clean` and malformed records to `clickstream-dlq`.

## Current Entry Point

```bash
python streaming/replay.py --help
```

## Service URLs

After running the relevant setup script, the local service UIs are available at:

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

## Local Broker

Start Docker Desktop, then run:

```bash
./infra/scripts/setup_m1.sh
```

This starts a local Redpanda broker and creates:

```text
clickstream-raw
clickstream-clean
clickstream-dlq
```

The local Kafka bootstrap server is:

```text
localhost:19092
```

Redpanda Console is available at:

```text
http://localhost:8080
```

Replay a small batch into Kafka:

```bash
python streaming/replay.py --rows 20 --speed 100x --sink kafka --no-sleep
```

Inspect records from the raw topic:

```bash
docker compose -f infra/compose/kafka.yml exec -T redpanda \
  rpk topic consume clickstream-raw --brokers localhost:9092 --offset start --num 20
```

To reset the M1 broker state:

```bash
./infra/scripts/reset_m1.sh
```

## Validation Layer

Start the M2 stack:

```bash
./infra/scripts/setup_m2.sh
```

This starts Redpanda, Redpanda Console, Flink JobManager/TaskManager, and the validation job.

```text
Flink UI: http://localhost:8081
Redpanda Console: http://localhost:8080
```

The validation job is defined in `streaming/flink_job_validation.sql`.

Report validation counts and DLQ rate:

```bash
./infra/scripts/verify_m2.sh
```

## Analytical Parquet Dataset

Start the M3 stack:

```bash
./infra/scripts/setup_m3.sh
```

This starts the validation layer plus the analytical Flink job that consumes `clickstream-clean` and writes partitioned Parquet output to:

```text
data/analytics/clickstream
```

Verify the partitioned dataset:

```bash
./infra/scripts/verify_m3.sh
```

The analytical job is defined in `streaming/flink_job_analytics.sql`.

For full October materialization, reset first to avoid duplicate append-only Kafka/Parquet output:

```bash
./infra/scripts/reset_m3.sh
./infra/scripts/setup_m3.sh
python3 streaming/replay.py --full --speed 10000x --sink kafka --no-sleep --corrupt-probability 0 --delay-probability 0 --quiet --progress-every 100000
./infra/scripts/verify_m3.sh
```

By default, the source dataset is stored at:

```text
data/source/2019-Oct.csv.gz
```

Generated runtime data under `data/` is ignored by Git.

## Active Architecture

```text
Replay Engine
      |
      v
Kafka: clickstream-raw
      |
      v
Validation Flink Job
      |------------------|
      v                  v
Kafka: clickstream-clean Kafka: clickstream-dlq
      |
      |------------------|
      v                  v
Analytical Flink     Operational Flink
      |                  |
      v                  v
   Parquet           PostgreSQL
                         |
                         v
                  Live Dashboard
```

See `planning/planning.md` for the full architecture and milestone plan.

## Exploration Archive

Earlier source-discovery experiments for Telegram, World Monitor, YouTube, and batch-vs-streaming research are preserved under `exploration/`. They are retained as research artifacts, but they are not part of the active clickstream bot-detection implementation.


# do not delete
```bash
rezwanhoppe-islam@Rezwans-MacBook-Pro project-3-streaming-bot-detection % docker compose -f infra/compose/kafka.yml \
  exec -T redpanda \
  rpk topic consume clickstream-raw \
  --brokers localhost:9092 \
  -o start \
  -n 5 \
  -f '{"partition":%p,"offset":%o,"event":%v}\n' \
| jq .
```
```json
{
  "partition": 0,
  "offset": 0,
  "event": {
    "event_time": "2019-10-01 00:00:00 UTC",
    "event_type": "view",
    "product_id": "44600062",
    "category_id": "2103807459595387724",
    "category_code": "",
    "brand": "shiseido",
    "price": "35.79",
    "user_id": "541312140",
    "user_session": "72d76fde-8bb3-4e00-8c23-a032dfed738c"
  }
}
```
