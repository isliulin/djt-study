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
    public void makeSomeMyEventToKafka() {
        String topic = PROPS.getProperty("topic.event", null);
        Producer<String, String> producer = KafkaUtils.createProducer(ConfigConstants.getKafkaProducerProps());
        LocalDateTime dateTime = LocalDateTime.parse("2022-01-01 00:00:00", DatePattern.NORM_DATETIME_FORMATTER);
        for (int i = 0; i < 20; i++) {
            String thisTime = dateTime.plusSeconds(i).format(DatePattern.NORM_DATETIME_FORMATTER);
            MyEvent event = new MyEvent();
            event.setId(String.valueOf(RandomUtils.getRandomNumber(0, 100)));
            event.setName("张三");
            event.setNum(1L);
            event.setTime(thisTime);

            KafkaUtils.sendMessage(producer, topic, RandomUtils.getUuid(), JSON.toJSONString(event), false);
            producer.flush();
            ThreadUtil.sleep(200);
        }
    }

    @Test
    public void startConsumer() {
        String topic = PROPS.getProperty("topic.event", null);
        Consumer<String, String> consumer = KafkaUtils.createConsumer(ConfigConstants.getKafkaConsumerProps());
        consumer.subscribe(Collections.singletonList(topic));
        KafkaUtils.startConsumer(topic, 1000, true, ConfigConstants.getKafkaConsumerProps());
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
