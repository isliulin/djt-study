package com.djt.utils;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.thread.ThreadUtil;
import com.alibaba.fastjson.JSON;
import com.djt.event.MyEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Collections;

import static com.djt.utils.ConfigConstants.PROPS;

/**
 * 造数据工具类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-08-04
 */
public class MakeDataUtils {

    /**
     * 起始时间
     * LocalDateTime.now();
     */
    public static final LocalDateTime TIME_START =
            LocalDateTime.parse("2021-01-01 00:00:00", DatePattern.NORM_DATETIME_FORMATTER);

    /**
     * 生成一条数据
     *
     * @param offset 偏移
     * @return JSONObject
     */
    public MyEvent makeOneMyEvent(int offset) {
        MyEvent event = new MyEvent();
        event.setId(RandomUtils.getUuid());
        event.setName(RandomUtils.getRandomName());
        event.setNum(RandomUtils.getRandomNumber(0L, 100));
        event.setTime(TIME_START.plusMinutes(offset).format(DatePattern.NORM_DATETIME_FORMATTER));
        return event;
    }

    /**
     * 发送一条数据
     */
    @Test
    public void makeOneMyEventToKafka() {
        MyEvent event = new MyEvent();
        event.setId("666");
        event.setName("张三");
        event.setNum(100L);
        event.setTime("2022-01-01 00:00:00");

        String topic = PROPS.getProperty("topic.event", null);
        Producer<String, String> producer = KafkaUtils.createProducer(ConfigConstants.getKafkaProducerProps());
        KafkaUtils.sendMessage(producer, topic, event.getId(), JSON.toJSONString(event));
        producer.flush();
    }

    @Test
    public void startConsumer() {
        String topic = PROPS.getProperty("topic.event", null);
        Consumer<String, String> consumer = KafkaUtils.createConsumer(ConfigConstants.getKafkaConsumerProps());
        consumer.subscribe(Collections.singletonList(topic));
        KafkaUtils.startConsumer(consumer, 1000, true);
    }

    @Test
    public void makeMyEventToKafka() {
        String topic = PROPS.getProperty("topic.event", null);
        Producer<String, String> producer = KafkaUtils.createProducer(ConfigConstants.getKafkaProducerProps());
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            MyEvent event = makeOneMyEvent(i);
            KafkaUtils.sendMessage(producer, topic, event.getId(), JSON.toJSONString(event));
            ThreadUtil.sleep(1000);
        }
    }

}
