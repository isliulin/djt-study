package com.djt.kafka;

import cn.hutool.core.thread.ThreadUtil;
import com.djt.utils.ConfigConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * kafka生产者
 *
 * @author 　djt317@qq.com
 * @since 　 2021-04-20
 */
@Slf4j
public class KafkaDemo {

    /**
     * 创建生产者
     *
     * @param props 配置信息
     * @return Producer
     */
    public static Producer<String, String> createProducer(Properties props) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, props.getProperty(
                ConfigConstants.Kafka.KAFKA_ACKS_CONFIG, "all"));
        kafkaProps.put(ProducerConfig.RETRIES_CONFIG, props.getProperty(
                ConfigConstants.Kafka.KAFKA_RETRIES_CONFIG, "0"));
        kafkaProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, props.getProperty(
                ConfigConstants.Kafka.KAFKA_COMPRESSION_TYPE_CONFIG, "snappy"));
        kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG, props.getProperty(
                ConfigConstants.Kafka.KAFKA_BATCH_SIZE_CONFIG, "100"));
        kafkaProps.put(ProducerConfig.LINGER_MS_CONFIG, props.getProperty(
                ConfigConstants.Kafka.KAFKA_LINGER_MS_CONFIG, "0"));
        kafkaProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, props.getProperty(
                ConfigConstants.Kafka.KAFKA_BUFFER_MEMORY_CONFIG, "33554432"));
        kafkaProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, props.getProperty(
                ConfigConstants.Kafka.KAFKA_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1"));
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty(
                ConfigConstants.Kafka.KAFKA_BOOTSTRAP_SERVERS_CONFIG));
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, props.getProperty(
                ConfigConstants.Kafka.KAFKA_KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer"));
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, props.getProperty(
                ConfigConstants.Kafka.KAFKA_VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer"));
        return new KafkaProducer<>(kafkaProps);
    }

    /**
     * 创建消费者
     *
     * @param props 配置信息
     * @return Consumer
     */
    public static Consumer<String, String> createConsumer(Properties props) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty(
                ConfigConstants.Kafka.KAFKA_BOOTSTRAP_SERVERS_CONFIG));
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, props.getProperty(
                ConfigConstants.Kafka.KAFKA_GROUP_ID_CONFIG));
        kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, props.getProperty(
                ConfigConstants.Kafka.KAFKA_ENABLE_AUTO_COMMIT_CONFIG, "true"));
        kafkaProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, props.getProperty(
                ConfigConstants.Kafka.KAFKA_MAX_POLL_INTERVAL_MS_CONFIG, "1000"));
        kafkaProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, props.getProperty(
                ConfigConstants.Kafka.KAFKA_MAX_POLL_RECORDS_CONFIG, "2"));
        kafkaProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, props.getProperty(
                ConfigConstants.Kafka.KAFKA_AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000"));
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, props.getProperty(
                ConfigConstants.Kafka.KAFKA_AUTO_OFFSET_RESET_CONFIG, "earliest"));
        kafkaProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, props.getProperty(
                ConfigConstants.Kafka.KAFKA_SESSION_TIMEOUT_MS_CONFIG, "30000"));
        kafkaProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, props.getProperty(
                ConfigConstants.Kafka.KAFKA_HEARTBEAT_INTERVAL_MS_CONFIG, "1000"));
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, props.getProperty(
                ConfigConstants.Kafka.KAFKA_KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer"));
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, props.getProperty(
                ConfigConstants.Kafka.KAFKA_VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer"));
        return new KafkaConsumer<>(kafkaProps);
    }

    /**
     * 生产数据
     *
     * @param producer 生产者
     * @param topic    主题
     * @param key      消息标识
     * @param value    消息体
     */
    public static void sendMessage(Producer<String, String> producer, String topic, String key, String value) {
        producer.send(new ProducerRecord<>(topic, key, value));
        log.info("生产数据=>topic:{} key:{} value:{}", topic, key, value);
    }

    /**
     * 启动消费者线程
     *
     * @param props       配置信息
     * @param pollMs      拉取时间
     * @param dealMs      处理时间
     * @param isPrintData 是否打印数据
     * @param topics      消费主题列表
     */
    public static void startConsumer(Properties props, long pollMs, long dealMs, boolean isPrintData, String... topics) {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        Consumer<String, String> consumer = createConsumer(props);
        boolean isAutoCommit = Boolean.parseBoolean(props.getProperty("kafka.enable.auto.commit", "true"));
        List<String> topicList = Arrays.asList(topics);
        consumer.subscribe(topicList);
        log.info("启动消费者，消费主题=>{}", topicList);
        pool.execute(() -> {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(pollMs);
                int count = records.count();
                log.info("收到消息条数:{}", count);
                if (isPrintData) {
                    for (ConsumerRecord<String, String> record : records) {
                        log.info("消费数据=>topic:{} offset:{} key:{} value:{}", record.topic(), record.offset(), record.key(), record.value());
                    }
                }

                //手动等待一段时间 模拟数据处理时间
                if (count > 0 && dealMs > 0) {
                    ThreadUtil.sleep(dealMs);
                }

                //若非自动提交 则手动提交
                if (!isAutoCommit) {
                    consumer.commitSync();
                }
            }
        });
        pool.shutdown();
        while (!pool.isTerminated()) {
        }
    }

}
