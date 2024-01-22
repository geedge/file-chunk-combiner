package com.zdjizhi.utils;

import com.zdjizhi.config.Configs;
import org.apache.flink.configuration.Configuration;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Properties;

public class KafkaCertUtil {
    /**
     * Kafka SASL认证端口
     */
    private static final String SASL_PORT = "9094";

    /**
     * Kafka SSL认证端口
     */
    private static final String SSL_PORT = "9095";

    /**
     * 根据连接信息端口判断认证方式。
     *
     * @param properties kafka 连接配置信息
     */
    public static void chooseCert(Properties properties, Configuration configuration) {
        if (configuration.get(Configs.KAFKA_BROKER).contains(SASL_PORT)) {
            properties.put("security.protocol", "SASL_PLAINTEXT");
            properties.put("sasl.mechanism", "PLAIN");
            properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username="
                    + configuration.get(Configs.KAFKA_USER) + " password=" + configuration.get(Configs.KAFKA_PIN) + ";");
        } else if (configuration.get(Configs.KAFKA_BROKER).contains(SSL_PORT)) {
            properties.put("security.protocol", "SSL");
            properties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
            properties.put("ssl.keystore.location", configuration.get(Configs.KAFKA_TOOLS_LIBRARY) + "keystore.jks");
            properties.put("ssl.keystore.password", configuration.get(Configs.KAFKA_PIN));
            properties.put("ssl.truststore.location", configuration.get(Configs.KAFKA_TOOLS_LIBRARY) + "truststore.jks");
            properties.put("ssl.truststore.password", configuration.get(Configs.KAFKA_PIN));
            properties.put("ssl.key.password", configuration.get(Configs.KAFKA_PIN));
        }
    }
}
