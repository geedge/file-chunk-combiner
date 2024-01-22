package com.zdjizhi.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class Configs {
    public static final ConfigOption<String> FLINK_JOB_NAME = ConfigOptions.key("flink.job.name")
            .stringType()
            .defaultValue("FILE-CHUNK-COMBINER")
            .withDescription("The name of job.");

    public static final ConfigOption<Integer> SOURCE_KAFKA_PARALLELISM = ConfigOptions.key("source.kafka.parallelism")
            .intType()
            .defaultValue(1);
    public static final ConfigOption<String> KAFKA_BROKER = ConfigOptions.key("source.kafka.broker")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> KAFKA_GROUP_ID = ConfigOptions.key("source.kafka.group.id")
            .stringType()
            .defaultValue("test1");
    public static final ConfigOption<String> KAFKA_TOPIC = ConfigOptions.key("source.kafka.topic")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<Boolean> KAFKA_ENABLE_AUTO_COMMIT = ConfigOptions.key("source.kafka.enable.auto.commit")
            .booleanType()
            .defaultValue(true);
    public static final ConfigOption<String> KAFKA_AUTO_OFFSET_RESET = ConfigOptions.key("source.kafka.auto.offset.reset")
            .stringType()
            .defaultValue("latest");
    public static final ConfigOption<String> KAFKA_SESSION_TIMEOUT_MS = ConfigOptions.key("source.kafka.session.timeout.ms")
            .stringType()
            .defaultValue("60000");
    public static final ConfigOption<String> KAFKA_MAX_POLL_RECORDS = ConfigOptions.key("source.kafka.max.poll.records")
            .stringType()
            .defaultValue("1000");
    public static final ConfigOption<String> KAFKA_MAX_PARTITION_FETCH_BYTES = ConfigOptions.key("source.kafka.max.partition.fetch.bytes")
            .stringType()
            .defaultValue("31457280");
    public static final ConfigOption<String> KAFKA_USER = ConfigOptions.key("source.kafka.user")
            .stringType()
            .defaultValue("admin");
    public static final ConfigOption<String> KAFKA_PIN = ConfigOptions.key("source.kafka.pin")
            .stringType()
            .defaultValue("galaxy2019");
    public static final ConfigOption<String> KAFKA_TOOLS_LIBRARY = ConfigOptions.key("source.kafka.tools.library")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<Integer> PARSE_MESSAGE_PACK_PARALLELISM = ConfigOptions.key("parse.message.pack.parallelism")
            .intType()
            .defaultValue(1);
    public static final ConfigOption<Integer> COMBINER_WINDOW_PARALLELISM = ConfigOptions.key("combiner.window.parallelism")
            .intType()
            .defaultValue(1);
    public static final ConfigOption<Long> COMBINER_WINDOW_TIME = ConfigOptions.key("combiner.window.time")
            .longType()
            .defaultValue(5L);
    public static final ConfigOption<Long> COMBINER_WINDOW_IDLE_TIME = ConfigOptions.key("combiner.window.idle.time")
            .longType()
            .defaultValue(5L);
    public static final ConfigOption<Long> COMBINER_WINDOW_KEY_MAX_CHUNK = ConfigOptions.key("combiner.window.key.max.chunk")
            .longType()
            .defaultValue(5L);

    public static final ConfigOption<Integer> SINK_HOS_PARALLELISM = ConfigOptions.key("sink.hos.parallelism")
            .intType()
            .defaultValue(1);
    public static final ConfigOption<String> SINK_HOS_ENDPOINT = ConfigOptions.key("sink.hos.endpoint")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> SINK_HOS_BUCKET = ConfigOptions.key("sink.hos.bucket")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> SINK_HOS_TOKEN = ConfigOptions.key("sink.hos.token")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<Integer> SINK_HOS_HTTP_MAX_TOTAL = ConfigOptions.key("sink.hos.http.max.total")
            .intType()
            .defaultValue(2000);
    public static final ConfigOption<Integer> SINK_HOS_HTTP_MAX_PER_ROUTE = ConfigOptions.key("sink.hos.http.max.per.route")
            .intType()
            .defaultValue(1000);
    public static final ConfigOption<Integer> SINK_HOS_HTTP_ERROR_RETRY = ConfigOptions.key("sink.hos.http.error.retry")
            .intType()
            .defaultValue(3);
    public static final ConfigOption<Integer> SINK_HOS_HTTP_CONNECT_TIMEOUT = ConfigOptions.key("sink.hos.http.connect.timeout")
            .intType()
            .defaultValue(10000);
    public static final ConfigOption<Integer> SINK_HOS_HTTP_REQUEST_TIMEOUT = ConfigOptions.key("sink.hos.http.request.timeout")
            .intType()
            .defaultValue(10000);
    public static final ConfigOption<Integer> SINK_HOS_HTTP_SOCKET_TIMEOUT = ConfigOptions.key("sink.hos.http.socket.timeout")
            .intType()
            .defaultValue(60000);

}
