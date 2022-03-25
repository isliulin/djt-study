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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.Predicate;

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

    public static long start = System.currentTimeMillis();

    /**
     * 生产数据
     *
     * @param producer 生产者
     * @param topic    主题
     * @param key      消息标识
     * @param value    消息体
     */
    public static void sendMessage(Producer<String, String> producer, String topic, String key, String value) {
        sendMessage(producer, topic, key, value, true);
    }

    /**
     * 生产数据
     *
     * @param producer 生产者
     * @param topic    主题
     * @param key      消息标识
     * @param value    消息体
     * @param isPrint  是否打印
     */
    public static void sendMessage(Producer<String, String> producer, String topic, String key, String value, boolean isPrint) {
        producer.send(new ProducerRecord<>(topic, key, value));
        if (isPrint) {
            log.info("生产数据=>topic:{} key:{} value:{}", topic, key, value);
        }
    }

    /**
     * 消费者运行标志
     */
    public static volatile AtomicBoolean isRunning = new AtomicBoolean(true);

    /**
     * 启动消费者打印数据
     *
     * @param topic       主题
     * @param pollMs      拉取时间
     * @param isPrintData 是否打印数据
     * @param properties  kafka配置
     */
    public static void startConsumer(String topic, long pollMs, boolean isPrintData, Properties properties) {
        startConsumer(topic, pollMs, isPrintData, properties, null, null);
    }

    /**
     * 启动消费者打印数据
     *
     * @param topic       主题
     * @param pollMs      拉取时间
     * @param isPrintData 是否打印数据
     * @param properties  kafka配置
     * @param filterFunc  过滤器
     * @param mapFunc     映射器
     */
    public static void startConsumer(String topic, long pollMs, boolean isPrintData, Properties properties,
                                     Predicate<String> filterFunc, Function<String, String> mapFunc) {
        Producer<String, String> consumerTmp = KafkaUtils.createProducer(getConsumerProps(properties));
        int pts = consumerTmp.partitionsFor(topic).size();
        consumerTmp.close();
        ExecutorService pool = ThreadUtil.newExecutor(pts + 1, pts + 1, pts * 2);
        LongAdder counter = new LongAdder();
        for (int i = 0; i < pts; i++) {
            pool.execute(() -> {
                Consumer<String, String> consumer = KafkaUtils.createConsumer(getConsumerProps(properties));
                consumer.subscribe(Collections.singletonList(topic));
                while (isRunning.get()) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollMs));
                    for (ConsumerRecord<String, String> record : records) {
                        counter.increment();
                        if (!isPrintData) {
                            continue;
                        }
                        String msg = record.value();
                        if (filterFunc != null && !filterFunc.test(msg)) {
                            continue;
                        }
                        if (mapFunc != null) {
                            msg = mapFunc.apply(msg);
                        }
                        log.info("消费数据=>topic:{} offset:{} key:{} value:{}",
                                record.topic(), record.offset(), record.key(), msg);
                    }
                    consumer.commitSync();
                }
                consumer.close();
            });
        }

        //如果不打印具体数据 则启动一个线程 每隔5秒打印消费总数
        if (!isPrintData) {
            pool.execute(() -> {
                long lastTime = System.currentTimeMillis();
                long lastNums = 0;
                int noDateCount = 0;
                while (isRunning.get()) {
                    long now = System.currentTimeMillis();
                    long nums = counter.longValue();
                    if (now - lastTime >= 5000 && nums > lastNums) {
                        lastNums = nums;
                        lastTime = now;
                        noDateCount = 0;
                        log.info("已消费数据量:{}", nums);
                    } else if (nums <= lastNums && ++noDateCount >= 10) {
                        log.info("数据已全部消费.");
                        isRunning.set(false);
                    }
                    ThreadUtil.sleep(1000);
                }
            });
        }

        pool.shutdown();
        while (!pool.isTerminated()) {
            ThreadUtil.sleep(1000);
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
