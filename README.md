# Data Engineering Portfolio

This portfolio documents a three-project progression from local analytical processing, to cloud deployment, to real-time streaming systems.

The projects use the same e-commerce clickstream domain to explore how a data platform changes as its operating requirements change:

1. **Build locally** - process 42 million events on commodity hardware with an open lakehouse stack.
2. **Migrate to cloud** - preserve the lakehouse architecture while adding infrastructure, orchestration, and managed compute.
3. **Extend to streaming** - turn historical clickstream analytics into real-time operational bot detection.

The focus is not on collecting tools. The focus is on understanding the architectural tradeoffs behind modern data systems: storage format, compute model, orchestration, state, reliability, portability, and operational feedback loops.

---

## Projects

| Project | Focus | What It Demonstrates |
| ------- | ----- | -------------------- |
| [Project 1: Local Lakehouse](project-1-local-lakehouse/) | Batch analytics and ML on a local lakehouse | Transform raw clickstream events into Iceberg tables, session-level features, XGBoost predictions, and Superset dashboards |
| [Project 2: Cloud Lakehouse](project-2-cloud-lakehouse/) | Cloud migration and reproducible infrastructure | Provision the lakehouse on Google Cloud with Terraform, Cloud Run, Dataproc Spark, Cloud Workflows, Polaris, dbt, and Superset |
| [Project 3: Streaming Lakehouse](project-3-streaming-bot-detection/) | Real-time stream processing and operational analytics | Replay clickstream data through Kafka and Flink to produce validated streams, Parquet analytics, live PostgreSQL scores, and Grafana dashboards |

---

## Project 1: Local Lakehouse

[Project 1](project-1-local-lakehouse/) builds a local lakehouse that turns raw October 2019 e-commerce clickstream events into machine-learning-ready session features and purchase-conversion predictions.

```text
Raw clickstream CSV
        |
        v
Iceberg lakehouse tables
        |
        v
Session-level feature store
        |
        v
XGBoost conversion model
        |
        v
Metrics and Superset dashboard
```

Core ideas:

- Process large analytical datasets locally with open-source tools.
- Use open table formats to separate storage from compute.
- Build curated session-level features from raw event data.
- Train and evaluate a conversion model from lakehouse tables.
- Present model metrics through an interactive dashboard.

---

## Project 2: Cloud Lakehouse

[Project 2](project-2-cloud-lakehouse/) migrates the Project 1 architecture to Google Cloud while preserving openness, portability, and reproducibility.

```text
Terraform
    |
    v
Google Cloud infrastructure
    |
    v
Cloud Workflows orchestration
    |
    v
Ephemeral Dataproc Spark pipeline
    |
    v
Iceberg tables, model artifacts, and Superset dashboard
```

Core ideas:

- Translate a local lakehouse into cloud-native infrastructure.
- Provision cloud resources with Terraform instead of manual setup.
- Use managed compute while keeping data in open formats.
- Run the pipeline through Cloud Workflows and temporary Spark clusters.
- Keep deployment, execution, and teardown reproducible.

---

## Project 3: Streaming Lakehouse

[Project 3](project-3-streaming-bot-detection/) extends the clickstream platform from historical analytics into real-time operational analytics.

```text
Historical clickstream replay
        |
        v
Kafka topics
        |
        v
Flink validation and bot scoring
        |
        +--> Parquet analytical output
        |
        +--> PostgreSQL operational scores
                 |
                 v
              Grafana dashboard
```

Core ideas:

- Replay historical clickstream data as a live event stream.
- Validate events and isolate malformed records in a dead-letter queue.
- Use event time, watermarks, and bounded buffering for late data.
- Maintain keyed Flink state for live per-session bot scoring.
- Recover from failures with checkpoints.
- Decouple producers and consumers with Kafka.
- Serve operational metrics through PostgreSQL and Grafana.

---

## Themes Across the Portfolio

Across the three projects, the same dataset is used to study different data engineering questions:

- **Batch vs streaming:** when historical processing is enough, and when operational analytics needs live state.
- **Local vs cloud:** what changes when the same architecture has to be provisioned, secured, and torn down in a cloud environment.
- **Open formats and portability:** why Parquet, Iceberg, dbt, and portable compute patterns matter.
- **Reliability:** how validation, dead-letter queues, checkpoints, replay, and idempotent writes keep pipelines recoverable.
- **Operational visibility:** how dashboards, logs, metrics, consumer lag, and backpressure make data systems inspectable.

The overall goal is to build systems that are not only functional, but understandable: each project follows one concrete workflow end to end, explains the design choices, and documents the tradeoffs that shaped the implementation.
