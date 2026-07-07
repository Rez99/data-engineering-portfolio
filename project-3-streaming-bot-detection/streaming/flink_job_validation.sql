SET 'execution.runtime-mode' = 'streaming';
SET 'pipeline.name' = 'm2-clickstream-validation';
SET 'parallelism.default' = '3';

SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout' = '300s';
SET 'execution.checkpointing.min-pause' = '5s';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
SET 'state.checkpoints.dir' = 'file:///opt/flink/data/flink/checkpoints/m2-validation';

SET 'restart-strategy.type' = 'fixed-delay';
SET 'restart-strategy.fixed-delay.attempts' = '10';
SET 'restart-strategy.fixed-delay.delay' = '5s';

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
  'format' = 'json'
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

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO clean_clickstream
  SELECT
    event_time,
    event_type,
    product_id,
    category_id,
    category_code,
    brand,
    price,
    user_id,
    user_session
  FROM (
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
    FROM raw_clickstream
  )
  WHERE event_time IS NOT NULL
    AND TRY_CAST(REPLACE(event_time, ' UTC', '') AS TIMESTAMP(3)) IS NOT NULL
    AND event_type IN ('view', 'cart', 'purchase', 'remove_from_cart')
    AND product_id IS NOT NULL AND product_id <> ''
    AND user_id IS NOT NULL AND user_id <> ''
    AND user_session IS NOT NULL AND user_session <> ''
    AND (price IS NULL OR price = '' OR TRY_CAST(price AS DOUBLE) IS NOT NULL);

  INSERT INTO clickstream_dlq
  SELECT
    payload AS original_payload,
    CASE
      WHEN event_time IS NULL OR event_time = '' THEN 'missing event_time'
      WHEN TRY_CAST(REPLACE(event_time, ' UTC', '') AS TIMESTAMP(3)) IS NULL THEN 'invalid event_time'
      WHEN event_type IS NULL OR event_type = '' THEN 'missing event_type'
      WHEN event_type NOT IN ('view', 'cart', 'purchase', 'remove_from_cart') THEN 'unsupported event_type'
      WHEN product_id IS NULL OR product_id = '' THEN 'missing product_id'
      WHEN user_id IS NULL OR user_id = '' THEN 'missing user_id'
      WHEN user_session IS NULL OR user_session = '' THEN 'missing user_session'
      WHEN price IS NOT NULL AND price <> '' AND TRY_CAST(price AS DOUBLE) IS NULL THEN 'invalid price'
      ELSE 'unknown validation failure'
    END AS failure_reason,
    CAST(CURRENT_TIMESTAMP AS STRING) AS processing_timestamp
  FROM (
    SELECT
      payload,
      JSON_VALUE(payload, '$.event_time') AS event_time,
      JSON_VALUE(payload, '$.event_type') AS event_type,
      JSON_VALUE(payload, '$.product_id') AS product_id,
      JSON_VALUE(payload, '$.price') AS price,
      JSON_VALUE(payload, '$.user_id') AS user_id,
      JSON_VALUE(payload, '$.user_session') AS user_session
    FROM raw_clickstream
  )
  WHERE NOT (
    event_time IS NOT NULL
    AND TRY_CAST(REPLACE(event_time, ' UTC', '') AS TIMESTAMP(3)) IS NOT NULL
    AND event_type IN ('view', 'cart', 'purchase', 'remove_from_cart')
    AND product_id IS NOT NULL AND product_id <> ''
    AND user_id IS NOT NULL AND user_id <> ''
    AND user_session IS NOT NULL AND user_session <> ''
    AND (price IS NULL OR price = '' OR TRY_CAST(price AS DOUBLE) IS NOT NULL)
  );
END;
