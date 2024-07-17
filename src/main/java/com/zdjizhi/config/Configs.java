package com.zdjizhi.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class Configs {
    public static final ConfigOption<String> FLINK_JOB_NAME = ConfigOptions.key("flink.job.name")
            .stringType()
            .defaultValue("FILE-CHUNK-COMBINER")
            .withDescription("The name of job.");

    public static final ConfigOption<String> KAFKA_BROKER = ConfigOptions.key("source.kafka.broker")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> KAFKA_GROUP_ID = ConfigOptions.key("source.kafka.group.id")
            .stringType()
            .defaultValue("agg_traffic_file_chunk_combine");
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

    public static final ConfigOption<String> MAP_FILTER_EXPRESSION = ConfigOptions.key("map.filter.expression")
            .stringType()
            .defaultValue("");

    public static final ConfigOption<Integer> COMBINER_WINDOW_PARALLELISM = ConfigOptions.key("combiner.window.parallelism")
            .intType()
            .defaultValue(1);
    public static final ConfigOption<Long> COMBINER_WINDOW_SIZE = ConfigOptions.key("combiner.window.size")
            .longType()
            .defaultValue(10L);
    public static final ConfigOption<Boolean> COMBINER_WINDOW_ENABLE_LAST_CHUNK_TRIGGER = ConfigOptions.key("combiner.window.enable.last.chunk.trigger")
            .booleanType()
            .defaultValue(true);

    public static final ConfigOption<String> SINK_TYPE = ConfigOptions.key("sink.type")
            .stringType()
            .defaultValue("hos");
    public static final ConfigOption<Integer> SINK_PARALLELISM = ConfigOptions.key("sink.parallelism")
            .intType()
            .defaultValue(1);
    public static final ConfigOption<Boolean> SINK_ASYNC = ConfigOptions.key("sink.async")
            .booleanType()
            .defaultValue(false);
    public static final ConfigOption<String> SINK_FILTER_EXPRESSION = ConfigOptions.key("sink.filter.expression")
            .stringType()
            .defaultValue("");
    public static final ConfigOption<Long> SINK_RATE_LIMIT_THRESHOLD = ConfigOptions.key("sink.rate.limit.threshold")
            .longType()
            .defaultValue(0L);
    public static final ConfigOption<String> SINK_RATE_LIMIT_EXCLUSION_EXPRESSION = ConfigOptions.key("sink.rate.limit.exclusion.expression")
            .stringType()
            .defaultValue("");

    public static final ConfigOption<String> SINK_HOS_ENDPOINT = ConfigOptions.key("sink.hos.endpoint")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> SINK_HOS_BUCKET = ConfigOptions.key("sink.hos.bucket")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> SINK_HOS_TOKEN = ConfigOptions.key("sink.hos.token")
            .stringType()
            .defaultValue("");
    public static final ConfigOption<Long> SINK_HOS_BATCH_SIZE = ConfigOptions.key("sink.hos.batch.size")
            .longType()
            .defaultValue(0L);
    public static final ConfigOption<Integer> SINK_HOS_BATCH_INTERVAL_MS = ConfigOptions.key("sink.hos.batch.interval.ms")
            .intType()
            .defaultValue(0);
    public static final ConfigOption<Integer> SINK_HOS_HEALTH_CHECK_INTERVAL_MS = ConfigOptions.key("sink.hos.health.check.interval.ms")
            .intType()
            .defaultValue(60000);

    public static final ConfigOption<Integer> SINK_HTTP_CLIENT_MAX_TOTAL = ConfigOptions.key("sink.http.client.max.total")
            .intType()
            .defaultValue(100);
    public static final ConfigOption<Integer> SINK_HTTP_CLIENT_MAX_PER_ROUTE = ConfigOptions.key("sink.http.client.max.per.route")
            .intType()
            .defaultValue(100);
    public static final ConfigOption<Integer> SINK_HTTP_CLIENT_RETRIES_NUMBER = ConfigOptions.key("sink.http.client.retries.number")
            .intType()
            .defaultValue(3);
    public static final ConfigOption<Integer> SINK_HTTP_CLIENT_CONNECT_TIMEOUT_MS = ConfigOptions.key("sink.http.client.connect.timeout.ms")
            .intType()
            .defaultValue(10000);
    public static final ConfigOption<Integer> SINK_HTTP_CLIENT_REQUEST_TIMEOUT_MS = ConfigOptions.key("sink.http.client.request.timeout.ms")
            .intType()
            .defaultValue(10000);
    public static final ConfigOption<Integer> SINK_HTTP_CLIENT_SOCKET_TIMEOUT_MS = ConfigOptions.key("sink.http.client.socket.timeout.ms")
            .intType()
            .defaultValue(60000);

    public static final ConfigOption<String> SINK_HBASE_ZOOKEEPER = ConfigOptions.key("sink.hbase.zookeeper")
            .stringType()
            .defaultValue("127.0.0.1");
    public static final ConfigOption<Integer> SINK_HBASE_RETRIES_NUMBER = ConfigOptions.key("sink.hbase.retries.number")
            .intType()
            .defaultValue(10);
    public static final ConfigOption<Integer> SINK_HBASE_RPC_TIMEOUT_MS = ConfigOptions.key("sink.hbase.rpc.timeout.ms")
            .intType()
            .defaultValue(600000);
    public static final ConfigOption<Integer> SINK_HBASE_CLIENT_WRITE_BUFFER = ConfigOptions.key("sink.hbase.client.write.buffer")
            .intType()
            .defaultValue(10485760);
    public static final ConfigOption<Integer> SINK_HBASE_CLIENT_IPC_POOL_SIZE = ConfigOptions.key("sink.hbase.client.ipc.pool.size")
            .intType()
            .defaultValue(3);
    public static final ConfigOption<Long> SINK_HBASE_BATCH_SIZE = ConfigOptions.key("sink.hbase.batch.size")
            .longType()
            .defaultValue(0L);
    public static final ConfigOption<Integer> SINK_HBASE_BATCH_INTERVAL_MS = ConfigOptions.key("sink.hbase.batch.interval.ms")
            .intType()
            .defaultValue(0);

    public static final ConfigOption<String> SINK_OSS_ENDPOINT = ConfigOptions.key("sink.oss.endpoint")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> SINK_OSS_FILTER_EXPRESSION = ConfigOptions.key("sink.oss.filter.expression")
            .stringType()
            .defaultValue("");
    public static final ConfigOption<Long> SINK_OSS_CACHE_TIME_MS = ConfigOptions.key("sink.oss.cache.time.ms")
            .longType()
            .defaultValue(0L);
    public static final ConfigOption<Long> SINK_OSS_CACHE_SIZE = ConfigOptions.key("sink.oss.cache.size")
            .longType()
            .defaultValue(0L);
    public static final ConfigOption<String> FILE_META_FILTER_EXPRESSION = ConfigOptions.key("file.meta.filter.expression")
            .stringType()
            .defaultValue("");
    public static final ConfigOption<String> KAFKA_FILE_META_SESSION_TOPIC = ConfigOptions.key("source.kafka.file.meta.session.topic")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> KAFKA_FILE_META_PROXY_TOPIC = ConfigOptions.key("source.kafka.file.meta.proxy.topic")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> KAFKA_FILE_META_GROUP_ID = ConfigOptions.key("source.kafka.file.meta.group.id")
            .stringType()
            .defaultValue("file_chunk_combine_1");
}
