package com.portfolio.botdetection;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

public class ClickstreamValidationJob {
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "redpanda:9092";
    private static final String DEFAULT_SOURCE_TOPIC = "clickstream-raw";
    private static final String DEFAULT_CLEAN_TOPIC = "clickstream-clean";
    private static final String DEFAULT_DLQ_TOPIC = "clickstream-dlq";
    private static final String DEFAULT_GROUP_ID = "m2-validation";
    private static final OutputTag<DlqRecord> DLQ_TAG = new OutputTag<>("clickstream-dlq") {
    };

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.parallelism);
        env.enableCheckpointing(60_000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(300_000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000L);
        env.getCheckpointConfig().setCheckpointStorage("file:///opt/flink/datasets/flink-checkpoints/m2-validation");
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                org.apache.flink.streaming.api.environment.CheckpointConfig
                        .ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(config.bootstrapServers)
                .setTopics(config.sourceTopic)
                .setGroupId(config.groupId)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        SingleOutputStreamOperator<CleanRecord> cleanRecords = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "clickstream-raw")
                .process(new ValidationFunction())
                .name("parse-and-validate-clickstream");

        DataStream<DlqRecord> dlqRecords = cleanRecords.getSideOutput(DLQ_TAG);

        cleanRecords.sinkTo(KafkaSink.<CleanRecord>builder()
                .setBootstrapServers(config.bootstrapServers)
                .setRecordSerializer(new CleanRecordSerializer(config.cleanTopic))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build()).name("clean-clickstream-sink");

        dlqRecords.sinkTo(KafkaSink.<DlqRecord>builder()
                .setBootstrapServers(config.bootstrapServers)
                .setRecordSerializer(new DlqRecordSerializer(config.dlqTopic))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build()).name("clickstream-dlq-sink");

        env.execute("m2-clickstream-validation");
    }

    private static class ValidationFunction extends ProcessFunction<String, CleanRecord> {
        @Override
        public void processElement(String payload, Context context, Collector<CleanRecord> out) {
            CleanRecord record = CleanRecord.fromPayload(payload);
            String failureReason = record.failureReason();
            if (failureReason == null) {
                out.collect(record);
            } else {
                context.output(DLQ_TAG, new DlqRecord(payload, failureReason));
            }
        }
    }

    public static class CleanRecord implements Serializable {
        public String eventTime;
        public String eventType;
        public String productId;
        public String categoryId;
        public String categoryCode;
        public String brand;
        public String price;
        public String userId;
        public String userSession;

        public static CleanRecord fromPayload(String payload) {
            CleanRecord record = new CleanRecord();
            record.eventTime = jsonString(payload, "event_time");
            record.eventType = jsonString(payload, "event_type");
            record.productId = jsonString(payload, "product_id");
            record.categoryId = jsonString(payload, "category_id");
            record.categoryCode = jsonString(payload, "category_code");
            record.brand = jsonString(payload, "brand");
            record.price = jsonString(payload, "price");
            record.userId = jsonString(payload, "user_id");
            record.userSession = jsonString(payload, "user_session");
            return record;
        }

        public String failureReason() {
            if (isMissing(eventTime)) {
                return "missing event_time";
            }
            if (!isValidEventTime(eventTime)) {
                return "invalid event_time";
            }
            if (isMissing(eventType)) {
                return "missing event_type";
            }
            if (!("view".equals(eventType)
                    || "cart".equals(eventType)
                    || "purchase".equals(eventType)
                    || "remove_from_cart".equals(eventType))) {
                return "unsupported event_type";
            }
            if (isMissing(productId)) {
                return "missing product_id";
            }
            if (isMissing(userId)) {
                return "missing user_id";
            }
            if (isMissing(userSession)) {
                return "missing user_session";
            }
            if (price != null && !price.isEmpty() && !isValidDouble(price)) {
                return "invalid price";
            }
            return null;
        }

        public String toJson() {
            return "{"
                    + "\"event_time\":" + jsonValue(eventTime)
                    + ",\"event_type\":" + jsonValue(eventType)
                    + ",\"product_id\":" + jsonValue(productId)
                    + ",\"category_id\":" + jsonValue(categoryId)
                    + ",\"category_code\":" + jsonValue(categoryCode)
                    + ",\"brand\":" + jsonValue(brand)
                    + ",\"price\":" + jsonValue(price)
                    + ",\"user_id\":" + jsonValue(userId)
                    + ",\"user_session\":" + jsonValue(userSession)
                    + "}";
        }
    }

    public static class DlqRecord implements Serializable {
        public String originalPayload;
        public String failureReason;

        public DlqRecord() {
        }

        public DlqRecord(String originalPayload, String failureReason) {
            this.originalPayload = originalPayload;
            this.failureReason = failureReason;
        }

        public String toJson() {
            return "{"
                    + "\"original_payload\":" + jsonValue(originalPayload)
                    + ",\"failure_reason\":" + jsonValue(failureReason)
                    + ",\"processing_timestamp\":" + jsonValue(DateTimeFormatter.ISO_INSTANT.format(Instant.now()))
                    + "}";
        }
    }

    private static class CleanRecordSerializer implements KafkaRecordSerializationSchema<CleanRecord> {
        private final String topic;

        private CleanRecordSerializer(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                CleanRecord record,
                KafkaSinkContext context,
                Long timestamp) {
            return new ProducerRecord<>(
                    topic,
                    record.userSession.getBytes(StandardCharsets.UTF_8),
                    record.toJson().getBytes(StandardCharsets.UTF_8));
        }
    }

    private static class DlqRecordSerializer implements KafkaRecordSerializationSchema<DlqRecord> {
        private final String topic;

        private DlqRecordSerializer(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                DlqRecord record,
                KafkaSinkContext context,
                Long timestamp) {
            return new ProducerRecord<>(topic, record.toJson().getBytes(StandardCharsets.UTF_8));
        }
    }

    private static boolean isMissing(String value) {
        return value == null || value.isEmpty();
    }

    private static boolean isValidEventTime(String eventTime) {
        if (eventTime.length() == 23
                && eventTime.charAt(4) == '-'
                && eventTime.charAt(7) == '-'
                && eventTime.charAt(10) == ' '
                && eventTime.charAt(13) == ':'
                && eventTime.charAt(16) == ':'
                && eventTime.endsWith(" UTC")
                && areDigits(eventTime, 0, 4)
                && areDigits(eventTime, 5, 7)
                && areDigits(eventTime, 8, 10)
                && areDigits(eventTime, 11, 13)
                && areDigits(eventTime, 14, 16)
                && areDigits(eventTime, 17, 19)) {
            int month = twoDigitInt(eventTime, 5);
            int day = twoDigitInt(eventTime, 8);
            int hour = twoDigitInt(eventTime, 11);
            int minute = twoDigitInt(eventTime, 14);
            int second = twoDigitInt(eventTime, 17);
            return month >= 1 && month <= 12
                    && day >= 1 && day <= 31
                    && hour <= 23
                    && minute <= 59
                    && second <= 59;
        }
        try {
            Instant.parse(eventTime.replace(" UTC", "Z").replace(" ", "T"));
            return true;
        } catch (RuntimeException exception) {
            return false;
        }
    }

    private static boolean areDigits(String value, int startInclusive, int endExclusive) {
        for (int index = startInclusive; index < endExclusive; index++) {
            char character = value.charAt(index);
            if (character < '0' || character > '9') {
                return false;
            }
        }
        return true;
    }

    private static int twoDigitInt(String value, int start) {
        return (value.charAt(start) - '0') * 10 + (value.charAt(start + 1) - '0');
    }

    private static boolean isValidDouble(String value) {
        try {
            Double.parseDouble(value);
            return true;
        } catch (NumberFormatException exception) {
            return false;
        }
    }

    private static String jsonString(String json, String key) {
        String needle = "\"" + key + "\"";
        int keyIndex = json.indexOf(needle);
        if (keyIndex < 0) {
            return null;
        }
        int colonIndex = json.indexOf(':', keyIndex + needle.length());
        if (colonIndex < 0) {
            return null;
        }
        int valueStart = colonIndex + 1;
        while (valueStart < json.length() && Character.isWhitespace(json.charAt(valueStart))) {
            valueStart++;
        }
        if (valueStart >= json.length() || json.charAt(valueStart) != '"') {
            return null;
        }
        valueStart++;
        StringBuilder escaped = null;
        for (int index = valueStart; index < json.length(); index++) {
            char character = json.charAt(index);
            if (character == '\\') {
                if (escaped == null) {
                    escaped = new StringBuilder(json.length() - valueStart);
                    escaped.append(json, valueStart, index);
                }
                if (index + 1 >= json.length()) {
                    return null;
                }
                char escapedCharacter = json.charAt(++index);
                switch (escapedCharacter) {
                    case '"' -> escaped.append('"');
                    case '\\' -> escaped.append('\\');
                    case '/' -> escaped.append('/');
                    case 'b' -> escaped.append('\b');
                    case 'f' -> escaped.append('\f');
                    case 'n' -> escaped.append('\n');
                    case 'r' -> escaped.append('\r');
                    case 't' -> escaped.append('\t');
                    default -> escaped.append(escapedCharacter);
                }
            } else if (character == '"') {
                if (escaped == null) {
                    return json.substring(valueStart, index);
                }
                return escaped.toString();
            } else if (escaped != null) {
                escaped.append(character);
            }
        }
        return null;
    }

    private static String jsonValue(String value) {
        if (value == null) {
            return "null";
        }
        return "\"" + escapeJsonString(value) + "\"";
    }

    private static String escapeJsonString(String value) {
        StringBuilder builder = new StringBuilder(value.length() + 16);
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            switch (character) {
                case '"' -> builder.append("\\\"");
                case '\\' -> builder.append("\\\\");
                case '\b' -> builder.append("\\b");
                case '\f' -> builder.append("\\f");
                case '\n' -> builder.append("\\n");
                case '\r' -> builder.append("\\r");
                case '\t' -> builder.append("\\t");
                default -> {
                    if (character < 0x20) {
                        builder.append(String.format("\\u%04x", (int) character));
                    } else {
                        builder.append(character);
                    }
                }
            }
        }
        return builder.toString();
    }

    private static class JobConfig {
        public String bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;
        public String sourceTopic = DEFAULT_SOURCE_TOPIC;
        public String cleanTopic = DEFAULT_CLEAN_TOPIC;
        public String dlqTopic = DEFAULT_DLQ_TOPIC;
        public String groupId = DEFAULT_GROUP_ID;
        public int parallelism = 4;

        static JobConfig fromArgs(String[] args) {
            JobConfig config = new JobConfig();
            for (int index = 0; index < args.length; index++) {
                String arg = args[index];
                if ("--bootstrap-servers".equals(arg)) {
                    config.bootstrapServers = args[++index];
                } else if ("--source-topic".equals(arg)) {
                    config.sourceTopic = args[++index];
                } else if ("--clean-topic".equals(arg)) {
                    config.cleanTopic = args[++index];
                } else if ("--dlq-topic".equals(arg)) {
                    config.dlqTopic = args[++index];
                } else if ("--group-id".equals(arg)) {
                    config.groupId = args[++index];
                } else if ("--parallelism".equals(arg)) {
                    config.parallelism = Integer.parseInt(args[++index]);
                }
            }
            return config;
        }
    }
}
