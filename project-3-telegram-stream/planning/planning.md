# Table of Contents
- [1. Exploration](#1-exploration)
  - [1.1 What features should a streaming project demonstrate?](#11-what-features-should-a-streaming-project-demonstrate)
  - [1.2 What domain should I pick?](#12-what-domain-should-i-pick)
  - [1.3 Why I chose the real-time clickstream project](#13-why-i-chose-the-real-time-clickstream-project)
  - [1.4 High-level (Kappa) architecture](#14-high-level-kappa-architecture)
- [2. Implementation sketch](#2-implementation-sketch)
  - [2.1 Producers -> Kafka](#21-producers---kafka)
    - [2.1.1 Pseudo-logic for `replay.py`](#211-pseudo-logic-for-replaypy)
    - [2.1.2 Mermaid diagram illustrating the replay logic](#212-mermaid-diagram-illustrating-the-replay-logic)
    - [2.1.3 Example console output to verify success](#213-example-console-output-to-verify-success)
  - [2.2 Kafka → Flink → Parquet](#22-kafka--flink--parquet)
  - [2.3 Kafka → Flink → Postgres](#23-kafka--flink--postgres)
    - [2.3.1 Session-level bot scoring](#231-session-level-bot-scoring)
    - [2.3.2 Stream-level bot scoring](#232-stream-level-bot-scoring)
    - [2.3.3 Historical normalization lookup table](#233-historical-normalization-lookup-table)
    - [2.3.4 Session state](#234-session-state)
- [3. Milestones](#3-milestones)
  - [3.1 Milestone Summary](#31-milestone-summary)
  - [3.2 Running the milestones](#32-running-the-milestones)
  - [3.3 Running the pipeline](#33-running-the-pipeline)
  - [3.4 M1: Producer → Kafka](#34-m1-producer--kafka)
  - [3.5 M2: Kafka → Flink → Parquet](#35-m2-kafka--flink--parquet)
  - [3.6 M3: Kafka → Flink → Postgres](#36-m3-kafka--flink--postgres)
  - [3.7 M4: Live Dashboard](#37-m4-live-dashboard)
  - [3.8 M5: Observability](#38-m5-observability)

# 1. Exploration
## 1.1 What features should a streaming project demonstrate?

> "If I were to demonstrate a true streaming project, then what components, features, capabilities would you expect to see? What does a strong streaming project look like?"

```text
Streaming Project

├── 1. Data Flow
│     ├── Unbounded event stream
│     ├── Message broker (Kafka)
│     ├── Multiple producers
│     └── Multiple consumers
│
├── 2. Stream Processing
│     ├── Stateful processing
│     ├── Windowing
│     ├── Event time + watermarks
│     └── Non-trivial transformations
│
├── 3. Outputs
│     ├── Live serving
│     ├── Persistent storage
│     └── Replay
│
├── 4. Reliability
│     ├── Offset management
│     ├── Delivery semantics
│     └── Dead letter queue
│
├── 5. Operations
│     ├── Consumer lag
│     ├── Throughput & latency
│     ├── Backpressure
│     └── Horizontal scaling
│
└── 6. Nice to Have
      └── Schema evolution
```

## 1.2 What domain should I pick?

> Now, can you add another two columns to this table? Each column will be for a different domain.The first domain will be basically a continuation of Project 1 and Project 2. You know how we used the October dataset, the web, the clickstream dataset. I'm guessing we could do something with that, like, for example, identify bots in real time. Because if you remember, we removed sessions or users who had a very large number of events per session post hoc. But we could do this in real time. We could, I guess, generate a number of metrics, which might look like robots. That's what I'm thinking. If you have better ideas, by all means speak up. But I want you to add a column and put this domain in there, and then tell me what each of these rows in the table would look like for that domain. And then for the other column, the other domain, that would be a text ingestion process. I don't know whether it's a live news feed or it's blue sky firehose or something like that, but what could the project look like to meet all the essential capabilities?

| Area | Essential Capabilities | Why It Matters | **Domain A: Real-Time Bot Detection (Clickstream)** | **Domain B: Live Text Feed Processing** |
|---|---|---|---|---|
| **Prerequisite** | • Latency-sensitive use case<br>• Continuous, valuable data | Justifies streaming over batch | Bots cause real-time harm — skewed analytics, ad fraud, server load. Post-hoc detection means damage is already done. Session ends in minutes; you must score it while it's live. | Detect emerging stories and trends while they are still developing rather than after the news cycle has moved on. |
| **1. Data Flow** | • Unbounded event stream<br>• Message broker (Kafka/Redpanda)<br>• Multiple producers<br>• Multiple consumers<br>• Partitions & consumer groups | Event-driven architecture and scalable ingestion | Simulated producers per site section (homepage, search, product, checkout) emit click events continuously. Kafka partitioned by session_id. Consumers: session scorer, alert engine, dashboard feeder, raw storage writer. | Multiple independent feed producers (RSS, Bluesky firehose, news APIs) emit text events continuously. Kafka partitioned by topic/story hash — not source — so related content lands on the same partition. Consumers: enricher, topic state updater, velocity detector, raw storage writer. |
| **2. Stream Processing** | • Stateful processing<br>• Windowing (tumbling/sliding/session)<br>• Event time & watermarks<br>• Non-trivial transformations | True streaming semantics, not simple ETL | Session windows accumulate per-user state: events/min, click entropy, page repeat rate, navigation speed, impossible click intervals, purchase oscillation. Watermarks handle late mobile events. Output: bot probability score per session, continuously updated. | Tumbling windows count mentions per topic per 5-min bucket. Sliding windows compute velocity (rate of change). Stateful topic tracking detects emergence, acceleration, and decay. Watermarks handle feed delays and out-of-order publication timestamps. Transformation: topic state update, not per-article NLP. |
| **3. Outputs** | • Live dashboard / API / alerts<br>• Persistent storage (Iceberg/Parquet)<br>• Replay capability | Makes the stream useful downstream | Dashboard: live bot rate, top suspicious sessions, risk heatmap, score distribution, anomaly spikes. Iceberg stores raw events + session scores for model retraining. Replay lets you re-score historical sessions after rule or model updates. | Dashboard: emerging topics, story velocity, coverage by source, entity co-occurrence shifts. Iceberg stores raw articles + topic state snapshots. Replay lets new consumers bootstrap full topic history from retained offsets. |
| **4. Reliability** | • Offset management<br>• At-least-once / exactly-once semantics<br>• Dead letter queue | Correctness and resilience under failure | Exactly-once matters: duplicate events inflate velocity scores and cause false positives. Offset management ensures scorer resumes mid-session after crash. DLQ captures malformed events (null session_id, missing timestamps). | At-least-once acceptable: duplicate articles deduped by URL hash before state update. Offset management ensures topic state resumes correctly after crash. DLQ captures unparseable feed payloads or encoding errors. |
| **5. Operations** | • Consumer lag monitoring<br>• Throughput & latency metrics<br>• Backpressure handling<br>• Horizontal scaling | Production readiness and observability | Lag on scorer = delayed bot flags, bots escape the session window. Scale partitions during traffic spikes. Latency target: score updated within session window (~60s). Alert if lag exceeds one window length. | Lag on topic updater = stale trending topics, missed emergence window. Scale during breaking news when feed volume spikes. Latency target: topic state updated within one tumbling window (~5 min). NLP enrichment is the likely bottleneck — scale that consumer independently. |
| **6. Nice to Have** | • Schema evolution (Avro/Protobuf)<br>• Schema Registry | Long-lived streams without breaking consumers | Click event schema evolves as new UI ships — new event types, device fields, campaign tags. Avro + Registry prevents downstream consumers breaking on schema changes. | Feed schemas vary by source and evolve over time. Registry enforces a canonical event envelope across all producers regardless of source format. |

---

## 1.3 Why I chose the real-time clickstream project

I considered two domains for Project 3:

* Real-time clickstream analytics (bot detection)
* Live news/text stream processing

I chose the clickstream project for two reasons.

First, it provides a natural continuation of Projects 1 and 2, completing a clear progression from historical analytics, to cloud analytics, to real-time analytics.

Second, it keeps the scope focused on the learning objective: streaming data engineering. The clickstream domain is primarily a technical implementation problem, allowing me to focus on Kafka, stateful processing, windowing, event-time semantics, replay, and operational reliability.

A live news pipeline would also require solving a separate set of research problems—defining story boundaries, distinguishing news from opinion, merging reports from multiple publishers, and tracking how stories evolve over time. Those challenges are interesting, but they would significantly expand the scope beyond the core streaming concepts.

The news project remains a strong candidate for a future project once the streaming foundations are in place.

## 1.4 High-level (Kappa) architecture
```text
             Clickstream Evens                           
                     │                                   
                     ▼                                   
                   Kafka                                 
                     │                                   
                     ▼                                   
                Flink Jobs                               
                     │                                   
          ┌──────────┘──────────┐                        
          │                     │                        
          ▼                     ▼                        
 Analytical pipeline    Operational pipeline            
          │                     │                        
          ▼                     ▼                        
       Parquet              Postgres                     
          ⋮                     │                        
          ⋮                     ▼                        
      (Iceberg)*          Live Dashboard                  
                                                         
                                                         
    * Iceberg integration demonstrated in Projects 1 & 2.
```
```text
DATA FLOW

          Clickstream Events
                  │
                  ▼
               Kafka
          (System of Record)
          (producer offset — latest message written
           consumer offset — latest message committed
           lag = producer offset - consumer offset)
                  │
        ┌─────────┴─────────┐
        ▼                   ▼
  Iceberg Sink         Bot Scorer
        │                   │
        ▼                   ▼
 Historical          Current Session State
 Analytics              (Postgres)
                            │
                            ▼
                    Live Bot Dashboard


                 OBSERVABILITY

Kafka ──► JMX Exporter ──► Prometheus ──► Grafana
     (translates Kafka's              │
      internal metrics                ├── Consumer Lag
      into Prometheus format)         │   (rising = backpressure)
                                      └── Throughput
                                          (events/sec over time)
```
Diagram 1 — System Architecture    (this)
Diagram 2 — Bot Scorer Internals   (state, windows, event time, watermarks)
Diagram 3 — Deployment             (Docker Compose containers)


[Kappa Architecture](https://medium.com/@FrankAdams7/what-is-the-difference-between-lambda-and-kappa-architectures-3806be298089)
Kappa treats both batch and real-time workloads as stream processing problems. It uses the speed layer solely to prepare data for real-time and batch access. The architecture comprises two main layers:
- Speed or Stream: This layer often includes tiered storage, where all incoming data is stored indefinitely in different storage layers, such as S3 or GCS for historical data and on-disk logs for hot data.
- Serving: Transformations performed in the speed layer are not duplicated in the batch layer. Some complex transformations, like complex joins, may be pushed to the batch storage for implementation.

# 2. Implementation sketch
## 2.1 Producers -> Kafka

### 2.1.1 Pseudo-logic for `replay.py`
The goal of this component is to simulate a live clickstream by replaying historical events from the October 2019 dataset into Kafka. Rather than publishing the entire CSV as quickly as possible, the replay engine should emit events according to their original event timestamps, scaled by a configurable replay speed (e.g. 100×). The implementation should also support configurable network delay simulation, causing a subset of events to arrive out of order. Events are routed to dedicated producer workers based on their event type (view, cart, or purchase) before being published to Kafka. The following pseudo-logic describes the expected behaviour; the implementation does not need to match it line-for-line, but should preserve the same observable behaviour.
```text
1. Declare configuration
    - Replay speed e.g. 100x
    - Maximum rows e.g. 100,000 (or full dataset)
    - Event delay probability e.g. 1%
    - Mean event delay e.g. 5s
    - Random seed e.g. 1

2. Derive runtime state
    - Loop delay e.g. 1s
    - Cursor increment = Replay speed × Loop delay
    - Event timestamp cursor = earliest event timestamp

3. Start producer workers
    - View Producer
    - Cart Producer
    - Purchase Producer

4. Repeat until end of CSV

    4.1 Prepare eligible events for this replay tick
        - eligible_events = []
        - While next CSV event timestamp <= Event timestamp cursor
            - Add next CSV event to eligible_events
            - Read next CSV event

    4.2 Add simulated delay to eligible events
        - For each event in eligible_events
            - delay = fn(Event delay probability, Mean event delay)
            - send_timestamp = event_timestamp + delay

    4.3 Sort prepared events
        - Sort eligible_events by send_timestamp

    4.4 Dispatch prepared events

        - While eligible_events still has events

            - event = next event from sorted eligible_events

            - If event_type == "view"
                - View Producer.send(event)

            - If event_type == "cart"
                - Cart Producer.send(event)

            - If event_type == "purchase"
                - Purchase Producer.send(event)

    4.5 Advance replay time
        - Sleep(loop_delay)
        - Event timestamp cursor += cursor_increment
```

### 2.1.2 Mermaid diagram illustrating the replay logic
```mermaid
flowchart TD

    0([Start])
    1[Declare configuration]
    2[Derive runtime state]
    3[Start producers]

    0 --> 1 --> 2 --> 3

    subgraph ReplayLoop["Loop: Repeat until end of CSV"]

        subgraph PrepareLoop["Loop: Prepare eligible events"]

            4.1[Grab next event]
            4.1.1[Add to eligible events]
            4.1.2[Done]

            4.1 --> 4.1.1 --> 4.1
            4.1 --> 4.1.2

        end

        4.2[Add simulated delay]
        4.3[Sort by send timestamp]

        subgraph DispatchLoop["Loop: Dispatch prepared events"]

            4.4[Grab next event]
            D{Event type?}

            VP[View Producer]
            CP[Cart Producer]
            PP[Purchase Producer]

            4.4.2[Done]

            4.4 --> D
            D -->|View| VP
            D -->|Cart| CP
            D -->|Purchase| PP
            D -.-> 4.4

            D -->|No more events| 4.4.2

        end

        K[(Kafka)]

        4.1.2 --> 4.2 --> 4.3 --> 4.4

        VP --> K
        CP --> K
        PP --> K

        4.4.2 --> 4.5
        4.5[Advance replay time]
        4.5 -.-> 4.1

    end

    3 --> 4.1
    4.5 --> Done[Done]

    style 4.1.2 fill:#ff9999,color:white
    style 4.4.2 fill:#ff9999,color:white
    style 0 fill:green,color:white
    style Done fill:red,color:white

    style ReplayLoop fill:#FFF8DC,stroke:#999
    style PrepareLoop fill:#E6F7FF,stroke:#999
    style DispatchLoop fill:#F3E8FF,stroke:#999

```

### 2.1.3 Example console output to verify success
```text
Replay: 1× | Tick: 42

12:00:03.000  → 12:00:03.000   VIEW       ViewProducer
12:00:03.120  → 12:00:03.120   CART       CartProducer
12:00:03.250  → 12:00:08.250   PURCHASE   PurchaseProducer   [DELAYED]
12:00:04.010  → 12:00:04.010   VIEW       ViewProducer
12:00:03.800  → 12:00:08.800   VIEW       ViewProducer       [OUT OF ORDER]
```
where:

Event timestamp = when the event originally occurred.
Send timestamp = when your replay actually publishes it.
Producer = which worker handled it.
[DELAYED] = artificial network delay was applied.
[OUT OF ORDER] = this event arrives after a later event because of the simulated delay.

## 2.2 Kafka → Flink → Parquet
## 2.3 Kafka → Flink → Postgres
### 2.3.1 Session-level bot scoring
Recomputed on every new event using all events seen
for that session so far. Score accumulates confidence as the session grows.
Sessions scoring above 0.7 are classified as bots.

- `mean_click_interval_ms` — low values indicate bot-like activity
- `min_click_interval_ms` — bots can click faster than humans are physically capable of
- `sd_click_interval_ms` — bots are metronomic; humans are irregular

Each metric is normalized via percentile rank across all sessions in the past 24h.
The bot score is the straight average of the three normalized values:
```text
bot_score = mean(
percentile_rank(events_per_minute),
percentile_rank(1 / min_click_interval_ms),
percentile_rank(1 / sd_click_interval_ms)
)
```
### 2.3.2 Stream-level bot scoring
Computed continuously across all sessions in 5-min tumbling windows.

- Bot rate % — share of sessions scoring above 0.7 in the window
- Score histogram — distribution of bot scores across sessions in the window

### 2.3.3 Historical normalization lookup table
The historical October dataset is used to derive a normalization lookup table for the bot scoring algorithm. Click intervals are first computed by measuring the elapsed time between consecutive events within each session. The intervals are then aggregated by session to produce four statistics: mean, minimum, maximum, and standard deviation of click intervals. Finally, percentile distributions (P0–P100) are computed across all sessions for each statistic.

The resulting lookup table is generated once during offline analysis and stored as a static configuration artifact. When the Flink bot scoring job starts, the table is loaded into memory. As live session metrics are updated, the mean, minimum and standard deviation of click intervals are mapped to their corresponding historical percentiles to normalize each metric prior to computing the bot score. The historical distribution of maximum click intervals is used to select an appropriate session timeout (e.g. the 99th percentile), after which Flink safely evicts the session state from memory.


|   P | mean | min | max |  sd |
| --: | ---: | --: | --: | --: |
|   0 |  ... | ... | ... | ... |
|   1 |  ... | ... | ... | ... |
| ... |  ... | ... | ... | ... |
|  99 |  ... | ... | ... | ... |
| 100 |  ... | ... | ... | ... |

```sql
WITH events AS (

    SELECT
        user_session,
        event_time
    FROM events

),

click_intervals AS (

    SELECT
        user_session,
        EXTRACT(EPOCH FROM (
            event_time
            - LAG(event_time) OVER (
                PARTITION BY user_session
                ORDER BY event_time
            )
        )) * 1000 AS click_interval_ms
    FROM events

),

session_metrics AS (

    SELECT
        user_session,
        AVG(click_interval_ms)    AS mean_click_interval_ms,
        MIN(click_interval_ms)    AS min_click_interval_ms,
        MAX(click_interval_ms)    AS max_click_interval_ms,
        STDDEV(click_interval_ms) AS sd_click_interval_ms
    FROM click_intervals
    WHERE click_interval_ms IS NOT NULL
    GROUP BY user_session

)

SELECT
    p.percentile,

    percentile_cont(p.percentile)
        WITHIN GROUP (ORDER BY mean_click_interval_ms)
        AS mean_click_interval_ms,

    percentile_cont(p.percentile)
        WITHIN GROUP (ORDER BY min_click_interval_ms)
        AS min_click_interval_ms,

    percentile_cont(p.percentile)
        WITHIN GROUP (ORDER BY max_click_interval_ms)
        AS max_click_interval_ms,

    percentile_cont(p.percentile)
        WITHIN GROUP (ORDER BY sd_click_interval_ms)
        AS sd_click_interval_ms

FROM session_metrics
CROSS JOIN (
    SELECT generate_series(0,100) / 100.0 AS percentile
) p

GROUP BY p.percentile
ORDER BY p.percentile;
```

### 2.3.4 Session state

The bot scoring pipeline maintains a separate state object for each active session, keyed by `user_session`. Rather than storing the complete event history, the state stores only the information required to incrementally update the session statistics as new events arrive. This keeps the memory footprint constant with respect to session length while avoiding repeated recomputation.

| State Variable            |
| ------------------------- |
| `previous_timestamp`      |
| `interval_count`          |
| `interval_sum`            |
| `interval_min`            |
| `interval_sum_of_squares` |

From this compact state, the Flink job can derive the session statistics without retaining the complete event history:

* `mean = interval_sum / interval_count`
* `min = interval_min`
* `sd` is computed from `interval_count`, `interval_sum` and `interval_sum_of_squares`.

When a new event arrives, only the newly observed click interval is computed and incorporated into the running state before the updated state is written back.


# 3. Milestones
## 3.1 Milestone Summary

```mermaid
flowchart TD

    E[Clickstream Events]
    K[Kafka]
    F[Flink Jobs]

    subgraph ANALYTICAL["Analytical Pipeline"]
        PQ[Parquet]
        IB[(Iceberg)]
        PQ -.-> IB
    end

    subgraph OPERATIONAL["Operational Pipeline"]
        PG[Postgres]
        DB[Live Dashboard]
        PG -->|M4| DB
    end

    E -->|M1| K
    K --> F
    F -->|M2| PQ
    F -->|M3| PG
```

The project is implemented incrementally through a series of milestones. Each milestone extends the streaming pipeline while preserving the functionality developed in previous milestones.

| Milestone | Goal             | Primary Deliverable                                                                                                                           |
| --------- | ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| **M1**    | Producer → Kafka | Replay historical clickstream events into a Kafka topic, including configurable replay speed and simulated delayed/out-of-order events.       |
| **M2**    | Kafka → Flink → Parquet  | Persist the Kafka event stream as a partitioned Parquet dataset for downstream analytical processing.                                         |
| **M3**    | Kafka → Flink → Postgres | Compute live session-level bot detection metrics using stateful stream processing and continuously update an operational PostgreSQL database.   |
| **M4**    | Live Dashboard   | Visualize live bot detection metrics and operational health through an interactive dashboard.                                                 |
| **M5**    | Observability    | Monitor the streaming pipeline using operational metrics such as throughput, consumer lag, latency, and backpressure.   |
## 3.2 Running the milestones

Each milestone provides a pair of utility scripts:

* `setup_m*.sh` provisions the infrastructure required for that milestone.
* `reset_m*.sh` removes any generated state for that milestone, allowing it to be rerun from a clean starting point.

In addition, the project provides two convenience scripts:

* `setup_all.sh` provisions the complete streaming stack required to execute the full pipeline.
* `reset_all.sh` removes all generated state across every milestone while preserving source datasets, configuration, and project code.

The setup and reset scripts are idempotent and do **not** execute the data pipeline. Instead, they prepare and reset the development environment. Generated artifacts (such as Kafka topics, Flink checkpoints, Parquet output, and PostgreSQL tables) may be recreated repeatedly without affecting the source dataset or application code.
## 3.3 Running the pipeline

Once the desired infrastructure is running, the streaming pipeline is executed by starting the replay engine:

```bash
python replay.py [options]
```

For example:

```bash
python replay.py --rows 20 --speed 1x

python replay.py --full --speed 100x
```

| Milestone | Setup          | Reset          |
| --------- | -------------- | -------------- |
| M1        | `setup_m1.sh`  | `reset_m1.sh`  |
| M2        | `setup_m2.sh`  | `reset_m2.sh`  |
| M3        | `setup_m3.sh`  | `reset_m3.sh`  |
| M4        | `setup_m4.sh`  | `reset_m4.sh`  |
| M5        | `setup_m5.sh`  | `reset_m5.sh`  |
| All       | `setup_all.sh` | `reset_all.sh` |



## 3.4 M1: Producer → Kafka

**Goal:** Replay historical clickstream events into Kafka as a configurable real-time event stream.

| ID | Task | Acceptance Criteria |
|---|---|---|
| **M1.1** | Create replay application | - A Python application `replay.py` implementing the pseudo-logic described in [Section 2.1.1](#211-pseudo-logic-for-replaypy) executes successfully.<br>- The source CSV is downloaded automatically if it does not already exist.<br>- The source dataset is not re-downloaded if already present.<br>- Runtime configuration (e.g. replay speed, starting row, number of rows, debug mode) can be supplied via command-line arguments.<br>- The application is idempotent and can be executed repeatedly without overwriting the source dataset. |
| M1.2 | Implement replay engine | - A Python implementation of the replay engine described in Section 2.1 executes successfully.<br>- Running the replay at 1× speed produces a human-readable console trace demonstrating the expected replay behaviour, including event timestamp, send timestamp, event type, producer, and delayed/out-of-order events. <br>- Producer workers write to a temporary console sink, allowing routing and replay behaviour to be verified independently of Kafka.<br>- The replay engine preserves the complete source schema without adding, removing or modifying event fields prior to publication.|
| M1.3	| Deploy event broker	| - Kafka (or Redpanda) is running locally. <br>- A topic is created and ready to receive clickstream events. <br>- A retention policy appropriate for the streaming pipeline is configured.|
| M1.4	| Publish replay events	| - Producer workers publish replay events to the clickstream topic.<br>- A test consumer (or Kafka UI/CLI) verifies that all replay events are successfully received.<br>- Delayed events are observed arriving out of event-time order, demonstrating the replay engine's delay simulation. |

## 3.5 M2: Kafka → Flink → Parquet

**Goal:** Build a stateless Flink job that continuously persists the Kafka event stream as a partitioned Parquet dataset. The completed Parquet dataset serves as the historical analytical dataset used by subsequent milestones to derive bot scoring normalization parameters.

| ID | Task | Acceptance Criteria |
|----|------|---------------------|
| **M2.1** | Deploy Flink | - A local Flink cluster is running successfully.<br>- The environment is ready to execute streaming jobs. |
| **M2.2** | Implement analytical Flink job | - A Flink job continuously consumes the Kafka topic.<br>- Events are deserialized without modifying the source schema.<br>- No state, windows or watermarks are used. |
| **M2.3** | Write partitioned Parquet dataset | - The Flink job continuously writes events to a partitioned Parquet dataset.<br>- The dataset is partitioned by `event_date`. |
| **M2.4** | Verify analytical dataset | - Running the replay engine for a fixed number of events (e.g. 100,000) produces the same number of records in the partitioned Parquet dataset.<br>- Querying the dataset confirms that the schema is preserved and the directory structure is partitioned by event day. |
| **M2.5** | Materialize historical analytical dataset | - The replay engine is executed over the complete October dataset at an accelerated replay rate. <br>- The resulting partitioned Parquet dataset contains the complete historical clickstream dataset. <br>- The completed Parquet dataset is retained as a historical analytical dataset and serves as the input artifact for Milestone 3.|

## 3.6 M3: Kafka → Flink → Postgres

**Goal:** Build a stateful Flink job that continuously computes session-level bot detection metrics from the Kafka event stream and writes live bot scores to PostgreSQL. The job is parameterized using a historical normalization artifact derived from the analytical Parquet dataset produced in Milestone 2.

| ID | Task | Acceptance Criteria |
|----|------|---------------------|
| **M3.1** | Deploy DuckDB | - DuckDB is installed and accessible from the local development environment.<br>- The partitioned Parquet dataset produced in Milestone 2 can be queried successfully from DuckDB.<br>- A simple validation query confirms the expected row count and schema. |
| **M3.2** | Generate normalization artifact | - DuckDB queries the historical Parquet dataset produced in Milestone 2 using the SQL described in [Section 2.3.3](#233-historical-normalization-lookup-table).<br>- Session-level click interval statistics (`mean`, `min`, `max`, `sd`) are computed from the complete October dataset.<br>- Historical percentile distributions (P0–P100) are generated for each statistic and persisted as `normalization.parquet`.<br>- Bot scoring configuration values, including a session inactivity timeout equal to the 99th percentile of the historical maximum click interval distribution, are persisted as `bot_config.json`.<br>- Both artifacts can be loaded into memory by the Flink bot scoring job at startup.|
| **M3.3** | Implement stateful session aggregation | - A stateful Flink job continuously consumes the Kafka topic.<br>- Events are partitioned by `user_session` using `keyBy`.<br>- Each incoming event updates the compact session state described in [Section 2.3.4](#234-session-state), allowing the running click interval statistics (`mean`, `min`, `sd`) to be derived incrementally.<br>- Running `python replay.py --rows 20 --speed 1x` produces a human-readable debug trace demonstrating that the session state is updated correctly as successive events arrive for the same user_session.<br>- Session state expires after the configured inactivity timeout specified in `bot_config.json`. |
| **M3.4** | Compute **session-level** bot score | - `normalization.parquet` and `bot_config.json` are loaded during Flink job startup.<br>- The running session statistics (`mean`, `min`, `sd`) are mapped to their corresponding historical percentiles.<br>- A bot score is computed and updated for each incoming event. |
| **M3.5** | Compute **stream-level** bot metrics | - The stream of session-level bot scores is aggregated into the 5-minute tumbling windows described in [Section 2.3.2](#232-stream-level-bot-scoring).<br>- The bot rate and score histogram are computed continuously for each window.<br>- Running `python replay.py --rows 20 --speed 1x` demonstrates that stream-level metrics are updated as session bot scores change. |
| **M3.6** | Deploy PostgreSQL                | - A local PostgreSQL instance is running successfully.<br>- Tables for storing session-level bot scores and stream-level metrics are created. |
| **M3.7** | Persist operational metrics      | - Session-level bot scores are continuously written to PostgreSQL using an UPSERT operation (one row per active session).<br>- Stream-level metrics are continuously written to PostgreSQL.<br>- Running `python replay.py --rows 20 --speed 1x` demonstrates that both session-level bot scores and stream-level metrics are updated correctly in PostgreSQL.    

## 3.7 M4: Live Dashboard

**Goal:** Visualize the live operational state of the clickstream using continuously updated bot detection metrics.

| ID | Task | Acceptance Criteria |
|---|---|---|
| **M4.1** | Deploy Grafana                  | - Grafana is running locally.<br>- Grafana is connected to the PostgreSQL database produced in Milestone 3.                                                                                                                                                                                     |
| **M4.2** | Visualize session-level metrics | - A Grafana dashboard displays the current active sessions and their corresponding bot scores.<br>- The dashboard refreshes automatically as new events are processed.<br>- Running `python replay.py --rows 20 --speed 1x` demonstrates live updates to session-level bot scores.              |
| **M4.3** | Visualize stream-level metrics  | - A Grafana dashboard displays the stream-level metrics described in [Section 2.3.2](#232-stream-level-bot-scoring), including bot rate and score histogram.<br>- Running `python replay.py --rows 100000 --speed 100x` demonstrates the dashboard updating continuously as the stream evolves. |

## 3.8 M5: Observability

**Goal:** Demonstrate operational monitoring of the streaming pipeline using the built-in monitoring capabilities of Kafka and Flink.

| ID | Task | Acceptance Criteria |
|---|---|---|
| **M5.1** | Deploy monitoring tools | - Kafka UI and the Flink Web UI are running locally.<br>- Both tools are accessible from the development environment.                                                                                                                                                                                     |
| **M5.2** | Monitor Kafka           | - Kafka UI displays the clickstream topic and active consumer groups.<br>- Consumer lag and message throughput are visible while the replay engine is running.<br>- Running `python replay.py --rows 100000 --speed 100x` demonstrates the live Kafka metrics updating.                                   |
| **M5.3** | Monitor Flink           | - The Flink Web UI displays the running streaming job and operator graph.<br>- Operator throughput and backpressure metrics are visible while the replay engine is running.<br>- Increasing the replay speed demonstrates changes in throughput and, where applicable, backpressure within the Flink job. |
