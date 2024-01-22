package com.zdjizhi.kafka;

import com.zdjizhi.config.Configs;
import com.zdjizhi.utils.KafkaCertUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.*;

public abstract class KafkaConsumer extends ByteArrayDeserializationSchema {

    private static Properties createConsumerConfig(Configuration configuration) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", configuration.get(Configs.KAFKA_BROKER));
        properties.put("group.id", configuration.get(Configs.KAFKA_GROUP_ID));
        properties.put("session.timeout.ms", configuration.get(Configs.KAFKA_SESSION_TIMEOUT_MS));
        properties.put("max.poll.records", configuration.get(Configs.KAFKA_MAX_POLL_RECORDS));
        properties.put("max.partition.fetch.bytes", configuration.get(Configs.KAFKA_MAX_PARTITION_FETCH_BYTES));
        properties.put("partition.discovery.interval.ms", "10000");
        properties.put("auto.offset.reset", configuration.get(Configs.KAFKA_AUTO_OFFSET_RESET));
        properties.put("enable.auto.commit", configuration.get(Configs.KAFKA_ENABLE_AUTO_COMMIT));
        KafkaCertUtil.chooseCert(properties, configuration);
        return properties;
    }

    public static FlinkKafkaConsumer<String> stringConsumer(Configuration configuration) {
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(configuration.get(Configs.KAFKA_TOPIC),
                new SimpleStringSchema(), createConsumerConfig(configuration));
        //随着checkpoint提交，将offset提交到kafka
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
        //从消费组当前的offset开始消费
        kafkaConsumer.setStartFromGroupOffsets();
        return kafkaConsumer;
    }

    public static FlinkKafkaConsumer<byte[]> byteArrayConsumer(Configuration configuration) {
        FlinkKafkaConsumer<byte[]> kafkaConsumer = new FlinkKafkaConsumer<>(configuration.get(Configs.KAFKA_TOPIC),
                new ByteArrayDeserializationSchema(), createConsumerConfig(configuration));
        //随着checkpoint提交，将offset提交到kafka
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
        //从消费组当前的offset开始消费
        kafkaConsumer.setStartFromGroupOffsets();
        return kafkaConsumer;
    }

}
