CREATE TABLE IF NOT EXISTS session_bot_scores (
  user_session TEXT PRIMARY KEY,
  last_event_time TIMESTAMP NOT NULL,
  event_count BIGINT NOT NULL,
  interval_count BIGINT NOT NULL,
  mean_click_interval_ms DOUBLE PRECISION,
  min_click_interval_ms DOUBLE PRECISION,
  sd_click_interval_ms DOUBLE PRECISION,
  bot_score DOUBLE PRECISION NOT NULL,
  is_bot BOOLEAN NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS stream_bot_metrics (
  window_start TIMESTAMP NOT NULL,
  window_end TIMESTAMP NOT NULL,
  active_sessions BIGINT NOT NULL,
  bot_sessions BIGINT NOT NULL,
  bot_rate DOUBLE PRECISION NOT NULL,
  avg_bot_score DOUBLE PRECISION NOT NULL,
  score_histogram TEXT NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  PRIMARY KEY (window_start, window_end)
);
