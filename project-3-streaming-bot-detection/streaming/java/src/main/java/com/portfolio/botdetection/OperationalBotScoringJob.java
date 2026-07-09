package com.portfolio.botdetection;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OperationalBotScoringJob {
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "redpanda:9092";
    private static final String DEFAULT_SOURCE_TOPIC = "clickstream-clean";
    private static final String DEFAULT_GROUP_ID = "m4-operational";
    private static final String DEFAULT_JDBC_URL = "jdbc:postgresql://postgres:5432/clickstream";
    private static final String DEFAULT_JDBC_USER = "clickstream";
    private static final String DEFAULT_JDBC_PASSWORD = "clickstream";
    private static final String DEFAULT_BOT_CONFIG_PATH = "/opt/flink/datasets/reference/bot_config.json";
    private static final String DEFAULT_NORMALIZATION_VALUES_PATH =
            "/opt/flink/generated/normalization_values.csv";

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromArgs(args);
        BotConfig botConfig = BotConfig.load(config.botConfigPath, config.normalizationValuesPath);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.parallelism);
        env.enableCheckpointing(60_000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(300_000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000L);
        env.getCheckpointConfig().setCheckpointStorage("file:///opt/flink/datasets/flink-checkpoints/m4-operational");
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(config.bootstrapServers)
                .setTopics(config.sourceTopic)
                .setGroupId(config.groupId)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        WatermarkStrategy<ClickEvent> watermarkStrategy = WatermarkStrategy
                .<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withIdleness(Duration.ofSeconds(30))
                .withTimestampAssigner(
                        (SerializableTimestampAssigner<ClickEvent>) (event, timestamp) -> event.eventTimeMillis);

        DataStream<ClickEvent> events = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "clickstream-clean")
                .flatMap((String payload, Collector<ClickEvent> out) -> {
                    ClickEvent event = ClickEvent.fromJson(payload);
                    if (event != null) {
                        out.collect(event);
                    }
                })
                .returns(ClickEvent.class)
                .assignTimestampsAndWatermarks(watermarkStrategy);

        DataStream<SessionUpdate> sessionUpdates = events
                .keyBy(event -> event.userSession)
                .process(new SessionScoringFunction(botConfig))
                .name("event-time-session-scoring");

        sessionUpdates.addSink(JdbcSink.sink(
                "INSERT INTO session_bot_scores "
                        + "(user_session, last_event_time, event_count, interval_count, "
                        + "mean_click_interval_ms, min_click_interval_ms, sd_click_interval_ms, "
                        + "bot_score, is_bot, updated_at, session_status, closed_at) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                        + "ON CONFLICT (user_session) DO UPDATE SET "
                        + "last_event_time = EXCLUDED.last_event_time, "
                        + "event_count = EXCLUDED.event_count, "
                        + "interval_count = EXCLUDED.interval_count, "
                        + "mean_click_interval_ms = EXCLUDED.mean_click_interval_ms, "
                        + "min_click_interval_ms = EXCLUDED.min_click_interval_ms, "
                        + "sd_click_interval_ms = EXCLUDED.sd_click_interval_ms, "
                        + "bot_score = EXCLUDED.bot_score, "
                        + "is_bot = EXCLUDED.is_bot, "
                        + "updated_at = EXCLUDED.updated_at, "
                        + "session_status = EXCLUDED.session_status, "
                        + "closed_at = EXCLUDED.closed_at",
                (statement, update) -> {
                    statement.setString(1, update.userSession);
                    statement.setTimestamp(2, new Timestamp(update.lastEventTimeMillis));
                    statement.setLong(3, update.eventCount);
                    statement.setLong(4, update.intervalCount);
                    setNullableDouble(statement, 5, update.meanClickIntervalMs);
                    setNullableDouble(statement, 6, update.minClickIntervalMs);
                    setNullableDouble(statement, 7, update.sdClickIntervalMs);
                    statement.setDouble(8, update.botScore);
                    statement.setBoolean(9, update.botScore >= botConfig.botScoreThreshold);
                    statement.setTimestamp(10, new Timestamp(System.currentTimeMillis()));
                    statement.setString(11, update.sessionStatus);
                    if (update.closedAtMillis == null) {
                        statement.setTimestamp(12, null);
                    } else {
                        statement.setTimestamp(12, new Timestamp(update.closedAtMillis));
                    }
                },
                jdbcExecutionOptions(),
                jdbcConnectionOptions(config)))
                .name("session-score-upserts");

        sessionUpdates
                .filter(update -> "active".equals(update.sessionStatus))
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(5)))
                .process(new BotRateWindowFunction(botConfig.botScoreThreshold))
                .name("event-time-bot-rate")
                .addSink(JdbcSink.sink(
                        "INSERT INTO stream_bot_metrics "
                                + "(window_start, window_end, active_sessions, bot_sessions, bot_rate, "
                                + "avg_bot_score, score_histogram, updated_at) "
                                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                                + "ON CONFLICT (window_start, window_end) DO UPDATE SET "
                                + "active_sessions = EXCLUDED.active_sessions, "
                                + "bot_sessions = EXCLUDED.bot_sessions, "
                                + "bot_rate = EXCLUDED.bot_rate, "
                                + "avg_bot_score = EXCLUDED.avg_bot_score, "
                                + "score_histogram = EXCLUDED.score_histogram, "
                                + "updated_at = EXCLUDED.updated_at",
                        (statement, metric) -> {
                            statement.setTimestamp(1, new Timestamp(metric.windowStartMillis));
                            statement.setTimestamp(2, new Timestamp(metric.windowEndMillis));
                            statement.setLong(3, metric.activeSessions);
                            statement.setLong(4, metric.botSessions);
                            statement.setDouble(5, metric.botRate);
                            statement.setDouble(6, metric.avgBotScore);
                            statement.setString(7, metric.scoreHistogram);
                            statement.setTimestamp(8, new Timestamp(System.currentTimeMillis()));
                        },
                        jdbcExecutionOptions(),
                        jdbcConnectionOptions(config)))
                .name("bot-rate-upserts");

        env.execute("m4-operational-bot-scoring");
    }

    private static JdbcExecutionOptions jdbcExecutionOptions() {
        return JdbcExecutionOptions.builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(1000)
                .withMaxRetries(3)
                .build();
    }

    private static JdbcConnectionOptions jdbcConnectionOptions(JobConfig config) {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(config.jdbcUrl)
                .withDriverName("org.postgresql.Driver")
                .withUsername(config.jdbcUser)
                .withPassword(config.jdbcPassword)
                .build();
    }

    private static void setNullableDouble(java.sql.PreparedStatement statement, int index, Double value)
            throws java.sql.SQLException {
        if (value == null || value.isNaN()) {
            statement.setNull(index, java.sql.Types.DOUBLE);
        } else {
            statement.setDouble(index, value);
        }
    }

    public static class SessionScoringFunction
            extends KeyedProcessFunction<String, ClickEvent, SessionUpdate> {
        private final BotConfig config;
        private transient ValueState<SessionState> state;

        SessionScoringFunction(BotConfig config) {
            this.config = config;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            state = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("active-session", SessionState.class));
        }

        @Override
        public void processElement(
                ClickEvent event,
                KeyedProcessFunction<String, ClickEvent, SessionUpdate>.Context context,
                Collector<SessionUpdate> out) throws Exception {
            SessionState current = state.value();

            if (current != null
                    && event.eventTimeMillis - current.lastEventTimeMillis > config.timeoutMillis) {
                out.collect(current.toUpdate("closed", current.closeTimerMillis));
                state.clear();
                current = null;
            }

            if (current == null) {
                current = SessionState.start(event.userSession, event.eventTimeMillis);
            } else {
                current.addEvent(event.eventTimeMillis);
            }

            current.botScore = config.score(current.meanClickIntervalMs(), current.minClickIntervalMs, current.sdClickIntervalMs());
            current.closeTimerMillis = current.lastEventTimeMillis + config.timeoutMillis;
            state.update(current);
            context.timerService().registerEventTimeTimer(current.closeTimerMillis);
            out.collect(current.toUpdate("active", null));
        }

        @Override
        public void onTimer(
                long timestamp,
                KeyedProcessFunction<String, ClickEvent, SessionUpdate>.OnTimerContext context,
                Collector<SessionUpdate> out) throws Exception {
            SessionState current = state.value();
            if (current != null && timestamp == current.closeTimerMillis) {
                out.collect(current.toUpdate("closed", timestamp));
                state.clear();
            }
        }
    }

    public static class BotRateWindowFunction
            extends ProcessAllWindowFunction<SessionUpdate, StreamMetric, TimeWindow> {
        private final double threshold;

        BotRateWindowFunction(double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void process(
                ProcessAllWindowFunction<SessionUpdate, StreamMetric, TimeWindow>.Context context,
                Iterable<SessionUpdate> updates,
                Collector<StreamMetric> out) {
            Map<String, SessionUpdate> latestBySession = new HashMap<>();
            for (SessionUpdate update : updates) {
                SessionUpdate existing = latestBySession.get(update.userSession);
                if (existing == null || update.lastEventTimeMillis >= existing.lastEventTimeMillis) {
                    latestBySession.put(update.userSession, update);
                }
            }

            long activeSessions = latestBySession.size();
            long botSessions = 0L;
            double scoreSum = 0.0;
            int[] buckets = new int[5];
            for (SessionUpdate update : latestBySession.values()) {
                if (update.botScore >= threshold) {
                    botSessions++;
                }
                scoreSum += update.botScore;
                int bucket = Math.min(4, Math.max(0, (int) Math.floor(update.botScore / 0.2)));
                buckets[bucket]++;
            }

            StreamMetric metric = new StreamMetric();
            metric.windowStartMillis = context.window().getStart();
            metric.windowEndMillis = context.window().getEnd();
            metric.activeSessions = activeSessions;
            metric.botSessions = botSessions;
            metric.botRate = activeSessions == 0 ? 0.0 : (double) botSessions / activeSessions;
            metric.avgBotScore = activeSessions == 0 ? 0.0 : scoreSum / activeSessions;
            metric.scoreHistogram = "0.0-0.2=" + buckets[0]
                    + ",0.2-0.4=" + buckets[1]
                    + ",0.4-0.6=" + buckets[2]
                    + ",0.6-0.8=" + buckets[3]
                    + ",0.8-1.0=" + buckets[4];
            out.collect(metric);
        }
    }

    public static class ClickEvent implements Serializable {
        public String userSession;
        public long eventTimeMillis;

        public ClickEvent() {
        }

        static ClickEvent fromJson(String payload) {
            String userSession = jsonString(payload, "user_session");
            String eventTime = jsonString(payload, "event_time");
            if (userSession == null || userSession.isBlank() || eventTime == null || eventTime.isBlank()) {
                return null;
            }

            ClickEvent event = new ClickEvent();
            event.userSession = userSession;
            event.eventTimeMillis = parseEventTimeMillis(eventTime);
            return event;
        }
    }

    public static class SessionState implements Serializable {
        public String userSession;
        public long firstEventTimeMillis;
        public long lastEventTimeMillis;
        public long eventCount;
        public long intervalCount;
        public double intervalSumMs;
        public double intervalSumSquaresMs;
        public Double minClickIntervalMs;
        public double botScore;
        public long closeTimerMillis;
        public ArrayList<Long> eventTimeMillis;

        public SessionState() {
        }

        static SessionState start(String userSession, long eventTimeMillis) {
            SessionState state = new SessionState();
            state.userSession = userSession;
            state.firstEventTimeMillis = eventTimeMillis;
            state.lastEventTimeMillis = eventTimeMillis;
            state.eventCount = 1L;
            state.eventTimeMillis = new ArrayList<>();
            state.eventTimeMillis.add(eventTimeMillis);
            return state;
        }

        void addEvent(long eventTimeMillis) {
            if (this.eventTimeMillis == null) {
                this.eventTimeMillis = new ArrayList<>();
                this.eventTimeMillis.add(lastEventTimeMillis);
            }

            int insertionPoint = Collections.binarySearch(this.eventTimeMillis, eventTimeMillis);
            if (insertionPoint < 0) {
                insertionPoint = -insertionPoint - 1;
            }
            this.eventTimeMillis.add(insertionPoint, eventTimeMillis);
            recomputeIntervalStats();
        }

        void recomputeIntervalStats() {
            eventCount = eventTimeMillis.size();
            firstEventTimeMillis = eventTimeMillis.get(0);
            lastEventTimeMillis = eventTimeMillis.get(eventTimeMillis.size() - 1);
            intervalCount = Math.max(0, eventCount - 1);
            intervalSumMs = 0.0;
            intervalSumSquaresMs = 0.0;
            minClickIntervalMs = null;

            for (int i = 1; i < eventTimeMillis.size(); i++) {
                double interval = eventTimeMillis.get(i) - eventTimeMillis.get(i - 1);
                intervalSumMs += interval;
                intervalSumSquaresMs += interval * interval;
                minClickIntervalMs = minClickIntervalMs == null ? interval : Math.min(minClickIntervalMs, interval);
            }
        }

        Double meanClickIntervalMs() {
            return intervalCount == 0 ? null : intervalSumMs / intervalCount;
        }

        Double sdClickIntervalMs() {
            if (intervalCount < 2) {
                return null;
            }
            double mean = intervalSumMs / intervalCount;
            double variance = Math.max(0.0,
                    (intervalSumSquaresMs - intervalCount * mean * mean) / (intervalCount - 1));
            return Math.sqrt(variance);
        }

        SessionUpdate toUpdate(String status, Long closedAtMillis) {
            SessionUpdate update = new SessionUpdate();
            update.userSession = userSession;
            update.lastEventTimeMillis = lastEventTimeMillis;
            update.eventCount = eventCount;
            update.intervalCount = intervalCount;
            update.meanClickIntervalMs = meanClickIntervalMs();
            update.minClickIntervalMs = minClickIntervalMs;
            update.sdClickIntervalMs = sdClickIntervalMs();
            update.botScore = botScore;
            update.sessionStatus = status;
            update.closedAtMillis = closedAtMillis;
            return update;
        }
    }

    public static class SessionUpdate implements Serializable {
        public String userSession;
        public long lastEventTimeMillis;
        public long eventCount;
        public long intervalCount;
        public Double meanClickIntervalMs;
        public Double minClickIntervalMs;
        public Double sdClickIntervalMs;
        public double botScore;
        public String sessionStatus;
        public Long closedAtMillis;

        public SessionUpdate() {
        }
    }

    public static class StreamMetric implements Serializable {
        public long windowStartMillis;
        public long windowEndMillis;
        public long activeSessions;
        public long botSessions;
        public double botRate;
        public double avgBotScore;
        public String scoreHistogram;

        public StreamMetric() {
        }
    }

    public static class BotConfig implements Serializable {
        public long timeoutMillis;
        public double botScoreThreshold;
        public NormalizationRow[] normalizationRows;

        static BotConfig load(String botConfigPath, String normalizationValuesPath) throws IOException {
            String botConfigJson = Files.readString(Path.of(botConfigPath), StandardCharsets.UTF_8);
            BotConfig config = new BotConfig();
            config.timeoutMillis = longJsonNumber(botConfigJson, "session_inactivity_timeout_ms");
            config.botScoreThreshold = doubleJsonNumber(botConfigJson, "bot_score_threshold");
            config.normalizationRows = loadNormalizationRows(normalizationValuesPath);
            return config;
        }

        double score(Double mean, Double min, Double sd) {
            if (mean == null || min == null || sd == null) {
                return 0.0;
            }
            return ((100 - percentileFor("mean", mean)) / 100.0
                    + (100 - percentileFor("min", min)) / 100.0
                    + (100 - percentileFor("sd", sd)) / 100.0) / 3.0;
        }

        private long percentileFor(String metric, double value) {
            long percentile = 0L;
            for (NormalizationRow row : normalizationRows) {
                double threshold = switch (metric) {
                    case "mean" -> row.meanClickIntervalMs;
                    case "min" -> row.minClickIntervalMs;
                    case "sd" -> row.sdClickIntervalMs;
                    default -> 0.0;
                };
                if (value >= threshold) {
                    percentile = Math.max(percentile, row.percentile);
                }
            }
            return percentile;
        }
    }

    public static class NormalizationRow implements Serializable {
        public long percentile;
        public double meanClickIntervalMs;
        public double minClickIntervalMs;
        public double sdClickIntervalMs;
    }

    public static class JobConfig {
        public String bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;
        public String sourceTopic = DEFAULT_SOURCE_TOPIC;
        public String groupId = DEFAULT_GROUP_ID;
        public String jdbcUrl = DEFAULT_JDBC_URL;
        public String jdbcUser = DEFAULT_JDBC_USER;
        public String jdbcPassword = DEFAULT_JDBC_PASSWORD;
        public String botConfigPath = DEFAULT_BOT_CONFIG_PATH;
        public String normalizationValuesPath = DEFAULT_NORMALIZATION_VALUES_PATH;
        public int parallelism = 3;

        static JobConfig fromArgs(String[] args) {
            JobConfig config = new JobConfig();
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                String value = i + 1 < args.length ? args[i + 1] : null;
                if ("--bootstrap-servers".equals(arg) && value != null) {
                    config.bootstrapServers = value;
                    i++;
                } else if ("--source-topic".equals(arg) && value != null) {
                    config.sourceTopic = value;
                    i++;
                } else if ("--group-id".equals(arg) && value != null) {
                    config.groupId = value;
                    i++;
                } else if ("--jdbc-url".equals(arg) && value != null) {
                    config.jdbcUrl = value;
                    i++;
                } else if ("--jdbc-user".equals(arg) && value != null) {
                    config.jdbcUser = value;
                    i++;
                } else if ("--jdbc-password".equals(arg) && value != null) {
                    config.jdbcPassword = value;
                    i++;
                } else if ("--bot-config".equals(arg) && value != null) {
                    config.botConfigPath = value;
                    i++;
                } else if ("--normalization-values".equals(arg) && value != null) {
                    config.normalizationValuesPath = value;
                    i++;
                } else if ("--parallelism".equals(arg) && value != null) {
                    config.parallelism = Integer.parseInt(value);
                    i++;
                }
            }
            return config;
        }
    }

    private static NormalizationRow[] loadNormalizationRows(String normalizationValuesPath) throws IOException {
        java.util.List<String> lines = Files.readAllLines(Path.of(normalizationValuesPath), StandardCharsets.UTF_8);
        java.util.ArrayList<NormalizationRow> rows = new java.util.ArrayList<>();
        for (String line : lines) {
            if (line.isBlank() || line.startsWith("percentile,")) {
                continue;
            }
            String[] parts = line.split(",", -1);
            if (parts.length < 6) {
                continue;
            }
            NormalizationRow row = new NormalizationRow();
            row.percentile = Long.parseLong(parts[0]);
            row.meanClickIntervalMs = Double.parseDouble(parts[2]);
            row.minClickIntervalMs = Double.parseDouble(parts[3]);
            row.sdClickIntervalMs = Double.parseDouble(parts[5]);
            rows.add(row);
        }
        if (rows.isEmpty()) {
            throw new IllegalStateException("No normalization values found in " + normalizationValuesPath);
        }
        return rows.toArray(new NormalizationRow[0]);
    }

    private static String jsonString(String json, String key) {
        Pattern pattern = Pattern.compile("\"" + Pattern.quote(key) + "\"\\s*:\\s*\"((?:\\\\.|[^\"])*)\"");
        Matcher matcher = pattern.matcher(json);
        if (!matcher.find()) {
            return null;
        }
        return matcher.group(1).replace("\\\"", "\"");
    }

    private static long longJsonNumber(String json, String key) {
        Pattern pattern = Pattern.compile("\"" + Pattern.quote(key) + "\"\\s*:\\s*([0-9]+)");
        Matcher matcher = pattern.matcher(json);
        if (!matcher.find()) {
            throw new IllegalStateException("Missing numeric config key: " + key);
        }
        return Long.parseLong(matcher.group(1));
    }

    private static double doubleJsonNumber(String json, String key) {
        Pattern pattern = Pattern.compile("\"" + Pattern.quote(key) + "\"\\s*:\\s*([0-9.]+)");
        Matcher matcher = pattern.matcher(json);
        if (!matcher.find()) {
            throw new IllegalStateException("Missing numeric config key: " + key);
        }
        return Double.parseDouble(matcher.group(1));
    }

    private static long parseEventTimeMillis(String eventTime) {
        String normalized = eventTime.replace(" UTC", "Z").replace(" ", "T");
        return Instant.parse(normalized).toEpochMilli();
    }
}
