# Operational Bot Scoring Architecture Comparison

This document compares the previous Flink SQL operational scoring job with the current Flink DataStream implementation.

## Summary

The operational bot-scoring pipeline consumes validated clickstream events from `clickstream-clean`, computes live session-level bot scores, writes those scores to PostgreSQL, and powers the Grafana dashboard.

The original implementation used Flink SQL. That was useful for proving the scoring model quickly, but it did not faithfully implement the operational session lifecycle:

```text
score active sessions in real time
close inactive sessions by event-time inactivity
clear Flink state for closed sessions
show active sessions separately from closed sessions
```

The current implementation uses a Java Flink DataStream job with keyed state and event-time timers. This better matches the operational requirement because session state is updated on every event and explicitly cleared when the event-time inactivity threshold is reached.

## Original Design Intent

M4 derives a session inactivity timeout from historical data. The timeout comes from `bot_config.json` and represents the point at which a session should be considered inactive.

The intended behavior is:

```text
For each user_session:
- update the bot score whenever a new event arrives
- keep the session active while events continue arriving within the inactivity threshold
- close the session when event_time has advanced past last_event_time + inactivity_timeout
- stop retaining Flink state for that closed session
```

This is an event-time rule. It is based on timestamps inside the clickstream data, not on laptop wall-clock time.

## Previous SQL Implementation

The previous operational job was implemented in `streaming/flink_job_operational.sql.template`.

It used:

```sql
SET 'table.exec.state.ttl' = '{{SESSION_STATE_TTL_SECONDS}} s';
```

and computed running session statistics with an unbounded ordered window:

```sql
PARTITION BY user_session
ORDER BY event_ts
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
```

It then wrote session scores and stream-level metrics to PostgreSQL through JDBC upsert sinks.

### What Worked

- It consumed `clickstream-clean`.
- It assigned event timestamps from `event_time`.
- It continuously updated session-level scores.
- It persisted results to PostgreSQL for Grafana.
- It used the M4-derived timeout as a Flink table state TTL.

### What Did Not Match The Requirement

`table.exec.state.ttl` is a processing-time state cleanup mechanism. It is not an event-time session close.

That means the SQL job did not actually say:

```text
close this session when event_time > last_event_time + inactivity_timeout
```

Instead, it said:

```text
Flink may eventually clean idle table state after the key has been idle in processing time.
```

That distinction matters during accelerated replay. With a high `--speed`, the replay can move through hours of historical event time in minutes of wall-clock time. A processing-time TTL may not fire while event time advances rapidly.

The SQL version also left PostgreSQL rows in `session_bot_scores` without an active/closed lifecycle marker, so Grafana could only count tracked rows, not active sessions.

## New DataStream Implementation

The current operational job is implemented in:

```text
streaming/java/src/main/java/com/portfolio/botdetection/OperationalBotScoringJob.java
```

It uses:

```text
KafkaSource -> event-time watermarks -> keyBy(user_session) -> KeyedProcessFunction -> JDBC sinks
```

For each session, the job keeps compact keyed state:

```text
first_event_time
last_event_time
event_count
interval_count
interval_sum_ms
interval_sum_squares_ms
min_click_interval_ms
current_bot_score
close_timer
```

On every event:

```text
1. update compact session statistics
2. recompute the bot score
3. write an active session update to PostgreSQL
4. register an event-time timer at last_event_time + inactivity_timeout
```

When the timer fires:

```text
1. emit a closed session update
2. set session_status = 'closed'
3. set closed_at
4. clear Flink keyed state
```

Grafana now counts active sessions with:

```sql
SELECT COUNT(*)
FROM session_bot_scores
WHERE session_status = 'active';
```

## Why The Change Was Made

The project needs both real-time updates and bounded session state.

A pure SQL session window would close sessions by event time, but it would naturally emit final results after the session closes. That is not enough for bot detection because the dashboard should identify suspicious sessions while they are still active.

The previous SQL running-window approach gave live updates, but did not explicitly close sessions by event-time inactivity.

The DataStream implementation supports both:

```text
live score updates while the session is active
event-time session closure after inactivity
state cleanup when the session closes
```

## Requirements Comparison

| Requirement | Previous SQL Job | New DataStream Job |
|---|---|---|
| Consume validated events from `clickstream-clean` | Yes | Yes |
| Score sessions continuously | Yes | Yes |
| Use M4 normalization artifacts | Yes | Yes |
| Use M4 inactivity threshold | Used as processing-time state TTL | Used as event-time session close threshold |
| Close sessions by event-time inactivity | No | Yes |
| Clear session state after close | Indirect TTL cleanup only | Explicit `state.clear()` on event-time timer |
| Distinguish active vs closed sessions in PostgreSQL | No | Yes, via `session_status` and `closed_at` |
| Keep live dashboard updates | Yes | Yes |
| Keep implementation simple | Simpler SQL | More code and build machinery |

## Tradeoffs

### Benefits Of The DataStream Version

- More faithful to the original M4 design.
- Session lifecycle is explicit and explainable.
- State is compact and bounded by event-time session closure.
- Grafana can show active sessions instead of cumulative tracked sessions.
- The operational job now models a real streaming state machine, which is stronger for a portfolio project.

### Costs Of The DataStream Version

- More code than SQL.
- Requires a Java build step.
- Adds a Maven project under `streaming/java`.
- The operational job is less editable directly from SQL.
- More implementation details must be tested and maintained.

## Why The New Design Better Matches Operational Requirements

Operational bot detection is not just a query. It is a stateful service behavior:

```text
watch active sessions
update risk as events arrive
expire inactive sessions using event time
preserve the latest operational view in PostgreSQL
```

That maps naturally to keyed state and event-time timers. The DataStream job makes the state lifecycle explicit, which is the important operational behavior.

The SQL job was useful for an earlier milestone because it demonstrated the scoring logic quickly. The DataStream job is a better final architecture because it represents how an operational streaming system should manage active entities over time.

## Current Operational Artifacts

The DataStream job uses these generated/runtime artifacts:

```text
batch/artifacts/bot_config.json
batch/artifacts/normalization.parquet
data/flink/generated/normalization_values.csv
data/flink/generated/operational-bot-scoring.jar
```

The jar is built during M4 setup:

```bash
./infra/scripts/infra_setup_m4.sh
```

`infra_setup_m4.sh` builds this jar before starting the operational job. `infra_setup_all.sh` reaches the same build step by running M4 in sequence.
