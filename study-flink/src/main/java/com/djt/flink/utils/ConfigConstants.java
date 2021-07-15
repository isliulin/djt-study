package com.djt.flink.utils;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * 配置参数名常量
 *
 * @author 　djt317@qq.com
 * @since 　 2021-07-14
 */
public interface ConfigConstants {


    /**
     * Kafka 配置
     */
    interface Kafka {

        /**
         * Kafka公共配置
         */
        String KAFKA_BOOTSTRAP_SERVERS_CONFIG = "kafka." + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

        /**
         * kafka生产者配置
         */
        String KAFKA_ACKS_CONFIG = "kafka." + ProducerConfig.ACKS_CONFIG;
        String KAFKA_RETRIES_CONFIG = "kafka." + ProducerConfig.RETRIES_CONFIG;
        String KAFKA_COMPRESSION_TYPE_CONFIG = "kafka." + ProducerConfig.COMPRESSION_TYPE_CONFIG;
        String KAFKA_BATCH_SIZE_CONFIG = "kafka." + ProducerConfig.BATCH_SIZE_CONFIG;
        String KAFKA_LINGER_MS_CONFIG = "kafka." + ProducerConfig.LINGER_MS_CONFIG;
        String KAFKA_BUFFER_MEMORY_CONFIG = "kafka." + ProducerConfig.BUFFER_MEMORY_CONFIG;
        String KAFKA_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = "kafka." + ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
        String KAFKA_KEY_SERIALIZER_CLASS_CONFIG = "kafka." + ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
        String KAFKA_VALUE_SERIALIZER_CLASS_CONFIG = "kafka." + ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

        /**
         * kafka消费者配置
         */
        String KAFKA_GROUP_ID_CONFIG = "kafka." + ConsumerConfig.GROUP_ID_CONFIG;
        String KAFKA_ENABLE_AUTO_COMMIT_CONFIG = "kafka." + ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
        String KAFKA_MAX_POLL_INTERVAL_MS_CONFIG = "kafka." + ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
        String KAFKA_MAX_POLL_RECORDS_CONFIG = "kafka." + ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
        String KAFKA_AUTO_COMMIT_INTERVAL_MS_CONFIG = "kafka." + ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;
        String KAFKA_AUTO_OFFSET_RESET_CONFIG = "kafka." + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
        String KAFKA_SESSION_TIMEOUT_MS_CONFIG = "kafka." + ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
        String KAFKA_HEARTBEAT_INTERVAL_MS_CONFIG = "kafka." + ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG;
        String KAFKA_KEY_DESERIALIZER_CLASS_CONFIG = "kafka." + ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
        String KAFKA_VALUE_DESERIALIZER_CLASS_CONFIG = "kafka." + ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
    }

    interface Socket {

        /**
         * flink socketTextStream host
         */
        String FLINK_SOCKET_STREAM_HOST = "flink.socket.stream.host";

        /**
         * flink socketTextStream port
         */
        String FLINK_SOCKET_STREAM_PORT = "flink.socket.stream.port";

        /**
         * flink socketTextStream delimiter
         */
        String FLINK_SOCKET_STREAM_DELIMITER = "flink.socket.stream.delimiter";
    }

}
