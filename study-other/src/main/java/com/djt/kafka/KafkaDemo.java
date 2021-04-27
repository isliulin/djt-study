package com.djt.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
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
        kafkaProps.put("acks", props.getProperty("kafka.acks", "all"));
        kafkaProps.put("retries", props.getProperty("kafka.retries", "0"));
        kafkaProps.put("compression.type", props.getProperty("kafka.compression.type", "snappy"));
        kafkaProps.put("batch.size", props.getProperty("kafka.batch.size", "100"));
        kafkaProps.put("linger.ms", props.getProperty("kafka.linger.ms", "0"));
        kafkaProps.put("buffer.memory", props.getProperty("kafka.buffer.memory", "33554432"));
        kafkaProps.put("max.in.flight.requests.per.connection", props.getProperty("kafka.max.in.flight.requests.per.connection", "1"));
        kafkaProps.put("bootstrap.servers", props.getProperty("kafka.bootstrap.servers"));
        kafkaProps.put("key.serializer", props.getProperty("kafka.key.serializer", "org.apache.kafka.common.serialization.StringSerializer"));
        kafkaProps.put("value.serializer", props.getProperty("kafka.value.serializer", "org.apache.kafka.common.serialization.StringSerializer"));
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
        kafkaProps.put("bootstrap.servers", props.getProperty("kafka.bootstrap.servers"));
        kafkaProps.put("group.id", props.getProperty("kafka.group.id"));
        kafkaProps.put("enable.auto.commit", props.getProperty("kafka.enable.auto.commit", "true"));
        kafkaProps.put("max.poll.interval.ms", props.getProperty("kafka.max.poll.interval.ms", "1000"));
        kafkaProps.put("max.poll.records", props.getProperty("kafka.max.poll.records", "2"));
        kafkaProps.put("auto.commit.interval.ms", props.getProperty("kafka.auto.commit.interval.ms", "1000"));
        kafkaProps.put("auto.offset.reset", props.getProperty("kafka.auto.offset.reset", "earliest"));
        kafkaProps.put("session.timeout.ms", props.getProperty("kafka.session.timeout.ms", "30000"));
        kafkaProps.put("heartbeat.interval.ms", props.getProperty("kafka.heartbeat.interval.ms", "1000"));
        kafkaProps.put("key.deserializer", props.getProperty("kafka.key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"));
        kafkaProps.put("value.deserializer", props.getProperty("kafka.value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"));
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
        log.info("生产数据=>topic:{} key:{} value:{}", topic, key, value);
        producer.send(new ProducerRecord<>(topic, key, value));
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
                    try {
                        Thread.sleep(dealMs);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                //若非自动提交 则手动提交
                if (!isAutoCommit) {
                    consumer.commitSync();
                }
            }
        });
        pool.shutdown();
    }

}
