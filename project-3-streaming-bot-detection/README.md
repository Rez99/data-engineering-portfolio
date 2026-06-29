# Project 3: Streaming Bot Detection

This project builds a real-time clickstream pipeline for bot detection. It replays the October 2019 e-commerce clickstream dataset into Kafka, validates raw events into a clean stream, persists analytical history to Parquet, and computes live session-level bot metrics for PostgreSQL and dashboarding.

The current implementation is at Milestone 1.3: the replay application can download or reuse the source CSV idempotently, accepts runtime replay options, emits a human-readable console replay trace through temporary producer sinks, and includes local Redpanda infrastructure for the raw, clean, and DLQ Kafka topics.

## Current Entry Point

```bash
python streaming/replay.py --help
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

To reset the M1 broker state:

```bash
./infra/scripts/reset_m1.sh
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
