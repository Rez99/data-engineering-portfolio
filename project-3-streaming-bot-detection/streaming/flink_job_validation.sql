SET 'execution.runtime-mode' = 'streaming';
SET 'pipeline.name' = 'm2-clickstream-validation';
SET 'parallelism.default' = '3';

SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout' = '300s';
SET 'execution.checkpointing.min-pause' = '5s';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
SET 'state.checkpoints.dir' = 'file:///opt/flink/datasets/flink-checkpoints/m2-validation';

SET 'restart-strategy.type' = 'fixed-delay';
SET 'restart-strategy.fixed-delay.attempts' = '10';
SET 'restart-strategy.fixed-delay.delay' = '5s';
SET 'table.optimizer.reuse-sub-plan-enabled' = 'true';
SET 'table.optimizer.reuse-source-enabled' = 'true';

CREATE TABLE raw_clickstream (
  payload STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'clickstream-raw',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'm2-validation',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'raw'
);

CREATE TABLE clean_clickstream (
  user_session_key STRING,
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
  'key.format' = 'raw',
  'key.fields' = 'user_session_key',
  'value.format' = 'json',
  'value.fields-include' = 'EXCEPT_KEY'
);

CREATE TABLE clickstream_dlq (
  original_payload STRING,
  failure_reason STRING,
  processing_timestamp STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'clickstream-dlq',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'format' = 'json'
);

CREATE TEMPORARY VIEW extracted_clickstream AS
SELECT
  payload,
  JSON_VALUE(payload, '$.event_time') AS event_time,
  JSON_VALUE(payload, '$.event_type') AS event_type,
  JSON_VALUE(payload, '$.product_id') AS product_id,
  JSON_VALUE(payload, '$.category_id') AS category_id,
  JSON_VALUE(payload, '$.category_code') AS category_code,
  JSON_VALUE(payload, '$.brand') AS brand,
  JSON_VALUE(payload, '$.price') AS price,
  JSON_VALUE(payload, '$.user_id') AS user_id,
  JSON_VALUE(payload, '$.user_session') AS user_session
FROM raw_clickstream;

CREATE TEMPORARY VIEW parsed_clickstream AS
SELECT
  payload,
  event_time,
  TRY_CAST(REPLACE(event_time, ' UTC', '') AS TIMESTAMP(3)) AS event_timestamp,
  event_type,
  product_id,
  category_id,
  category_code,
  brand,
  price,
  TRY_CAST(price AS DOUBLE) AS price_value,
  user_id,
  user_session
FROM extracted_clickstream;

CREATE TEMPORARY VIEW validated_clickstream AS
SELECT
  payload,
  event_time,
  event_type,
  product_id,
  category_id,
  category_code,
  brand,
  price,
  user_id,
  user_session,
  CASE
    WHEN event_time IS NULL OR event_time = '' THEN 'missing event_time'
    WHEN event_timestamp IS NULL THEN 'invalid event_time'
    WHEN event_type IS NULL OR event_type = '' THEN 'missing event_type'
    WHEN event_type NOT IN ('view', 'cart', 'purchase', 'remove_from_cart') THEN 'unsupported event_type'
    WHEN product_id IS NULL OR product_id = '' THEN 'missing product_id'
    WHEN user_id IS NULL OR user_id = '' THEN 'missing user_id'
    WHEN user_session IS NULL OR user_session = '' THEN 'missing user_session'
    WHEN price IS NOT NULL AND price <> '' AND price_value IS NULL THEN 'invalid price'
    ELSE NULL
  END AS failure_reason
FROM parsed_clickstream;

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO clean_clickstream
  SELECT
    user_session AS user_session_key,
    event_time,
    event_type,
    product_id,
    category_id,
    category_code,
    brand,
    price,
    user_id,
    user_session
  FROM validated_clickstream
  WHERE failure_reason IS NULL;

  INSERT INTO clickstream_dlq
  SELECT
    payload AS original_payload,
    failure_reason,
    CAST(CURRENT_TIMESTAMP AS STRING) AS processing_timestamp
  FROM validated_clickstream
  WHERE failure_reason IS NOT NULL;
END;
