package com.djt.utils;

import cn.hutool.core.thread.ThreadUtil;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAdder;

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
        return new KafkaProducer<>(getProducerProps(props));
    }

    /**
     * 创建消费者
     *
     * @param props 配置信息
     * @return Consumer
     */
    public static Consumer<String, String> createConsumer(Properties props) {
        return new KafkaConsumer<>(getConsumerProps(props));
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
     * 启动消费者
     *
     * @param topic       主题
     * @param pollMs      拉取时间
     * @param isPrintData 是否打印数据
     * @param properties  配置
     */
    public static void startConsumer(String topic, long pollMs, boolean isPrintData, Properties properties) {
        Producer<String, String> consumerTmp = KafkaUtils.createProducer(getConsumerProps(properties));
        int pts = consumerTmp.partitionsFor(topic).size();
        consumerTmp.close();
        ExecutorService pool = ThreadUtil.newExecutor(pts + 1, pts + 1, pts * 2);
        LongAdder counter = new LongAdder();
        for (int i = 0; i < pts; i++) {
            pool.execute(() -> {
                Consumer<String, String> consumer = KafkaUtils.createConsumer(getConsumerProps(properties));
                consumer.subscribe(Collections.singletonList(topic));
                while (isRunning) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollMs));
                    int rs = records.count();
                    counter.add(rs);
                    if (isPrintData) {
                        for (ConsumerRecord<String, String> record : records) {
                            log.info("消费数据=>topic:{} offset:{} key:{} value:{}",
                                    record.topic(), record.offset(), record.key(), record.value());
                        }
                    }
                    consumer.commitSync();
                }
                consumer.close();
            });
        }
        //启动一个线程 每隔5秒打印消费总数
        pool.execute(() -> {
            long lastTime = System.currentTimeMillis();
            long lastNums = 0;
            while (isRunning) {
                long now = System.currentTimeMillis();
                long nums = counter.longValue();
                if (now - lastTime >= 5000 && nums > lastNums) {
                    lastNums = nums;
                    lastTime = now;
                    log.info("已消费数据量:{}", nums);
                }
                ThreadUtil.sleep(1000);
            }
        });

        pool.shutdown();
        while (!pool.isTerminated()) {
            ThreadUtil.sleep(1);
        }
    }


    /**
     * 获取kafka consumer相关配置
     *
     * @return Properties
     */
    public static Properties getConsumerProps(Properties props) {
        Properties kafkaProps = new Properties();
        for (String key : ConsumerConfig.configNames()) {
            if (props.containsKey(key)) {
                kafkaProps.put(key, props.getProperty(key));
            }
        }
        kafkaProps.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        return kafkaProps;
    }

    /**
     * 获取kafka producer相关配置
     *
     * @return Properties
     */
    public static Properties getProducerProps(Properties props) {
        Properties kafkaProps = new Properties();
        for (String key : ProducerConfig.configNames()) {
            if (props.containsKey(key)) {
                kafkaProps.put(key, props.getProperty(key));
            }
        }

        kafkaProps.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        return kafkaProps;
    }
}
