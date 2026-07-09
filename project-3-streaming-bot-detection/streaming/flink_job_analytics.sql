SET 'execution.runtime-mode' = 'streaming';
SET 'pipeline.name' = 'm3-clean-clickstream-parquet';
SET 'parallelism.default' = '3';
SET 'execution.checkpointing.interval' = '60s';
SET 'execution.checkpointing.min-pause' = '30s';

CREATE TABLE clean_clickstream (
  event_time STRING,
  event_type STRING,
  product_id STRING,
  category_id STRING,
  category_code STRING,
  brand STRING,
  price STRING,
  user_id STRING,
  user_session STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'clickstream-clean',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'm3-analytics',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'json'
);

CREATE TABLE analytical_clickstream (
  event_time STRING,
  event_type STRING,
  product_id STRING,
  category_id STRING,
  category_code STRING,
  brand STRING,
  price STRING,
  user_id STRING,
  user_session STRING,
  event_date STRING
) PARTITIONED BY (event_date) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///opt/flink/datasets/analytics/clickstream',
  'format' = 'parquet',
  'sink.partition-commit.trigger' = 'process-time',
  'sink.partition-commit.delay' = '0s',
  'sink.partition-commit.policy.kind' = 'success-file',
  'sink.rolling-policy.file-size' = '64MB',
  'sink.rolling-policy.rollover-interval' = '1min',
  'sink.rolling-policy.check-interval' = '30s'
);

INSERT INTO analytical_clickstream
SELECT
  event_time,
  event_type,
  product_id,
  category_id,
  category_code,
  brand,
  price,
  user_id,
  user_session,
  DATE_FORMAT(TRY_CAST(REPLACE(event_time, ' UTC', '') AS TIMESTAMP(3)), 'yyyy-MM-dd') AS event_date
FROM clean_clickstream;
