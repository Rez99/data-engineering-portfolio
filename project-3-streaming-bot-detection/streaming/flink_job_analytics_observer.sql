SET 'execution.runtime-mode' = 'streaming';
SET 'pipeline.name' = 'm6-analytics-observer';
SET 'parallelism.default' = '1';
SET 'restart-strategy.type' = 'fixed-delay';
SET 'restart-strategy.fixed-delay.attempts' = '10';
SET 'restart-strategy.fixed-delay.delay' = '5s';

CREATE TABLE clean_clickstream (
  event_time STRING,
  event_type STRING,
  product_id STRING,
  category_id STRING,
  category_code STRING,
  brand STRING,
  price STRING,
  user_id STRING,
  user_session STRING,
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'clickstream-clean',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'm6-analytics-observer',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json'
);

CREATE TABLE analytics_observer_print (
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  event_count BIGINT,
  distinct_sessions BIGINT
) WITH (
  'connector' = 'print'
);

INSERT INTO analytics_observer_print
SELECT
  TUMBLE_START(proc_time, INTERVAL '1' MINUTE) AS window_start,
  TUMBLE_END(proc_time, INTERVAL '1' MINUTE) AS window_end,
  COUNT(*) AS event_count,
  COUNT(DISTINCT user_session) AS distinct_sessions
FROM clean_clickstream
GROUP BY TUMBLE(proc_time, INTERVAL '1' MINUTE);
