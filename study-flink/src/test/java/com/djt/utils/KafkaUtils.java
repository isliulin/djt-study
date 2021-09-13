package com.djt.utils;

import cn.hutool.core.thread.ThreadUtil;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Kafka工具类 用于造数据
 *
 * @author 　djt317@qq.com
 * @since 　 2021-08-10
 */
@Log4j2
public class KafkaUtils {

    /**
     * 创建生产者
     *
     * @param props 配置信息
     * @return Producer
     */
    public static Producer<String, String> createProducer(Properties props) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, props.getProperty(
                ProducerConfig.ACKS_CONFIG, "all"));
        kafkaProps.put(ProducerConfig.RETRIES_CONFIG, props.getProperty(
                ProducerConfig.RETRIES_CONFIG, "0"));
        kafkaProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, props.getProperty(
                ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"));
        kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG, props.getProperty(
                ProducerConfig.BATCH_SIZE_CONFIG, "100"));
        kafkaProps.put(ProducerConfig.LINGER_MS_CONFIG, props.getProperty(
                ProducerConfig.LINGER_MS_CONFIG, "0"));
        kafkaProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, props.getProperty(
                ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432"));
        kafkaProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, props.getProperty(
                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1"));
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, props.getProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer"));
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, props.getProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer"));
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
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, props.getProperty(
                ConsumerConfig.GROUP_ID_CONFIG));
        kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, props.getProperty(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"));
        kafkaProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, props.getProperty(
                ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "1000"));
        kafkaProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, props.getProperty(
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2"));
        kafkaProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, props.getProperty(
                ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000"));
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, props.getProperty(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
        kafkaProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, props.getProperty(
                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000"));
        kafkaProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, props.getProperty(
                ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000"));
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, props.getProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer"));
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, props.getProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer"));
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

    public static volatile boolean isRunning = true;

    /**
     * 启动消费者线程
     *
     * @param pollMs      拉取时间
     * @param isPrintData 是否打印数据
     */
    public static void startConsumer(Consumer<String, String> consumer, long pollMs, boolean isPrintData) {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        pool.execute(() -> {
            while (isRunning) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollMs));
                if (isPrintData) {
                    for (ConsumerRecord<String, String> record : records) {
                        log.info("消费数据=>topic:{} offset:{} key:{} value:{}",
                                record.topic(), record.offset(), record.key(), record.value());
                    }
                }
                consumer.commitSync();
            }
        });
        pool.shutdown();
        while (!pool.isTerminated()) {
            ThreadUtil.sleep(1);
        }
    }

}
