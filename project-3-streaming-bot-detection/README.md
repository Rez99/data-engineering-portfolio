# Project 3: Streaming Lakehouse

An end-to-end streaming data platform that transforms e-commerce clickstream events into real-time bot detection metrics, continuously updated analytical datasets, and live operational dashboards using open-source streaming technologies.

---

| Section | Contents |
| ------- | -------- |
| **[1. What This Project Does](#1-what-this-project-does)** | 1.1 Problem Statement<br>1.2 Inputs and Outputs<br>1.3 End-to-End Workflow |
| **[2. Follow One Session](#2-follow-one-deployment)** | 2.1 Infrastructure Provisioning<br>2.2 Platform Initialization<br>2.3 Pipeline Execution<br>2.4 Dashboard Publication |

# 1. What This Project Does
## 1.1 Problem Statement

The portfolio follows a three-stage progression:

1. **Project 1 — Build:** A modern local lakehouse capable of processing 42 million e-commerce clickstream events on commodity hardware using open-source technologies.
2. **Project 2 — Migrate:** The same architecture to the cloud while preserving openness, portability, and reproducibility.
3. **Project 3 — Extend:** The same clickstream dataset into a real-time streaming platform, evolving from historical analytics to operational analytics.

Unlike historical analytics, operational analytics enables organizations to act while events are still occurring rather than after they have already been collected and analyzed. Real-time bot detection serves as the demonstration application throughout this project. The value of real-time bot detection lies not in identifying bots itself, but in enabling immediate action across multiple business domains:

```mermaid
mindmap
  root((Real-Time<br/>Bot Detection))

    Advertising Fraud
      Prevent bots from clicking paid ads and inflating advertising costs.

    Analytics Quality
      Prevent bot traffic from distorting conversion rates, funnel metrics, A/B tests, and business KPIs.

    Website Performance
      Identify abusive traffic before it consumes server resources or triggers autoscaling.

    Rate Limiting & Security
      Throttle or block suspicious sessions before they scrape content or overwhelm APIs.

    Fraud Prevention
      Flag suspicious purchase or account creation behavior while the session is still active.

    Personalization
      Avoid feeding bot behavior into recommendation systems or customer profiles.
```

The project explores three questions:

1. How should historical batch analytics be adapted to stateful stream processing?
2. How can streaming systems be designed for reliability through replay, checkpointing, and fault tolerance?
3. How can analytical and operational workloads be supported from a single streaming pipeline?

## 1.2 Inputs and Outputs

### Inputs

The platform replays the October 2019 e-commerce clickstream dataset as a real-time event stream.

| Input | Purpose |
| ------ | ------- |
| October 2019 Clickstream Dataset | Simulate a production event stream for real-time processing |

### Outputs

The platform continuously produces the following artifacts:

| Output | Purpose |
| ------ | ------- |
| Clean Event Stream | Validated events for downstream consumers |
| Analytical Dataset | Persist historical event data for offline analytics |
| Operational Dashboard | Visualize live bot metrics and streaming health |

## 1.3 End-to-End Workflow
At a high level, the streaming platform supports two complementary workloads: historical analytics and real-time operational analytics.
```mermaid
flowchart TD

    DATA([Clickstream Data])

    PLATFORM[Streaming Data Platform]

    ANALYTICAL["Analytical Pipeline<br/><br/><i>(Historical Analysis)</i>"]

    OPERATIONAL["Operational Pipeline<br/><br/><i>(Real-Time Analysis)</i>"]

    DATA --> PLATFORM
    PLATFORM --> ANALYTICAL
    PLATFORM --> OPERATIONAL
```
The following workflow expands this view to show how replayed clickstream events flow through Kafka topics and Flink jobs to produce each output.
```mermaid
flowchart TD

    DATA([Clickstream Data])

    subgraph STREAMING["Streaming Data Platform"]

        REPLAY[Replay Engine]

        RAW[("Kafka Topic<br>Raw Clickstream")]

        VALIDATE["Flink Job<br>Schema Validation"]

        CLEAN[("Kafka Topic<br>Validated Clickstream")]

        DLQ[("Kafka Topic<br>Dead Letter Queue")]

        ANALYTICS_JOB["Flink Job<br>Parquet Writer"]

        OPERATIONAL_JOB["Flink Job<br>Bot Scorer"]

        REPLAY --> RAW
        RAW --> VALIDATE
        VALIDATE --> CLEAN
        VALIDATE --> DLQ

        CLEAN --> ANALYTICS_JOB
        CLEAN --> OPERATIONAL_JOB

    end

    subgraph ANALYTICAL["Analytical Pipeline"]

        PARQUET[(Parquet<br>Historical Data Store)]

        ICEBERG[(Iceberg*<br>Historical Data Store)]

        PARQUET -.-> ICEBERG

    end

    subgraph OP_PIPELINE["Operational Pipeline"]

        POSTGRES[(Postgres<br>Live Bot Scoring)]

        GRAFANA["Grafana<br>Live Dashboard"]

        POSTGRES --> GRAFANA

    end

    DATA --> REPLAY

    ANALYTICS_JOB --> PARQUET

    OPERATIONAL_JOB --> POSTGRES
```
`*` Iceberg integration demonstrated in Projects 1 & 2.

# 2. Follow One Session
```bash
docker compose -f infra/compose/duckdb.yml run --rm duckdb
```
```sql
SELECT
    user_session,
    COUNT(*) AS event_count,
    MIN(event_time) as min_event_time,
    MAX(event_time) as max_event_time
FROM read_csv_auto('/work/data/source/2019-Oct.csv.gz')
GROUP BY user_session
ORDER BY event_count DESC
LIMIT 5;
```
```sql
SELECT
    *
FROM read_csv_auto('/work/data/source/2019-Oct.csv.gz')
WHERE user_session = 'b2101293-44c1-4814-836a-94b0c03bb9c2';
```
---
```bash
docker compose -f infra/compose/postgres.yml exec postgres \
  psql -U clickstream -d clickstream
```  
```sql
SELECT
    user_session,
    event_count,
    bot_score,
    session_status,
    last_event_time
FROM session_bot_scores
WHERE is_bot IS TRUE
ORDER BY event_count DESC
LIMIT 20;
```
a3ecd197-a324-43bf-bfb0-f16e2820c23f



user_session = 'b2101293-44c1-4814-836a-94b0c03bb9c2' has 1159 events in 2019-Oct.csv.gz but has only 9 event count in session_bot_scores
clickstream=# select * from session_bot_scores where user_session = 'fb075266-182d-4c11-b5f7-4e4dcdabd4a7';
             user_session             |   last_event_time   | event_count | interval_count | mean_click_interval_ms | min_click_interval_ms | sd_click_interval_ms |      bot_score      | is_bot |       updated_at        | session_status | closed_at 
--------------------------------------+---------------------+-------------+----------------+------------------------+-----------------------+----------------------+---------------------+--------+-------------------------+----------------+-----------
 fb075266-182d-4c11-b5f7-4e4dcdabd4a7 | 2019-10-07 12:48:04 |           9 |              1 |                  60000 |                 60000 |                    0 | 0.47333333333333333 | f      | 2026-07-05 04:31:48.065 | active         | 

 how cam event count = 9, interval count  =1? wouldn't we excpect 8 intervals?
```sql
 WITH session_events AS (
    SELECT
        CAST(event_time AS TIMESTAMP) AS event_time
    FROM read_csv_auto('/work/data/source/2019-Oct.csv.gz')
    WHERE user_session = 'fb075266-182d-4c11-b5f7-4e4dcdabd4a7'
),
intervals AS (
    SELECT
        event_time,
        LAG(event_time) OVER (ORDER BY event_time) AS previous_event_time,
        EXTRACT(EPOCH FROM (
            event_time - LAG(event_time) OVER (ORDER BY event_time)
        )) AS interval_seconds
    FROM session_events
)
SELECT
    MAX(interval_seconds) AS max_interval_seconds
FROM intervals;
```