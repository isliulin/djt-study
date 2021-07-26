package com.djt.test.kafka;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.LocalDateTimeUtil;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.setting.dialect.PropsUtil;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.djt.kafka.KafkaDemo;
import com.djt.utils.DjtConstant;
import com.djt.utils.RandomUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-04-21
 */
@Slf4j
public class KafkaTest {

    protected Properties props;

    @Before
    public void before() {
        props = PropsUtil.get("config.properties");
    }

    @Test
    public void nothing() {

    }

    @Test
    public void testProduce() {
        String topic = "dc_etl_account";
        int size = 100;
        Producer<String, String> producer = KafkaDemo.createProducer(props);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DatePattern.PURE_DATETIME_PATTERN);
        for (int i = 0; i < size; i++) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("table", "CHARGE_USER.T_ACTIVITY_RECORD");
            jsonObject.put("op_type", "I");
            jsonObject.put("op_ts", LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_FORMATTER));
            jsonObject.put("current_ts", LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_FORMATTER));
            jsonObject.put("pos", System.currentTimeMillis());
            JSONObject data = new JSONObject();
            //String id = String.valueOf(System.currentTimeMillis());
            String id = String.valueOf(i);
            data.put("ID", id);
            data.put("ACTIVITY_ID", "11772299");
            data.put("AMOUNT", RandomUtils.getRandomNumber(0, 100));
            data.put("TRANS_TIME", RandomUtils.getRandomDate("20200101", "20210630", formatter));
            jsonObject.put("after", data);
            KafkaDemo.sendMessage(producer, topic, id, jsonObject.toJSONString());
        }
        producer.flush();
    }

    @Test
    public void testKafkaProducerETL() {
        String topic = "ETL_CHANGE_DATA";
        String table = "test.t_test_djt";
        Producer<String, String> producer = KafkaDemo.createProducer(props);
        for (int i = 0; i < 100; i++) {
            String time = LocalDateTime.now().format(DjtConstant.YMDHMSS_FORMAT);
            String timestamp = String.valueOf(System.currentTimeMillis());
            JSONObject message = new JSONObject();
            message.put("table", table);
            message.put("op_type", "U");
            message.put("op_ts", time);
            message.put("current_ts", time);
            message.put("pos", timestamp);
            message.put("before", new JSONObject());
            JSONObject after = new JSONObject();
            after.put("F1", String.valueOf(i));
            after.put("F2", String.valueOf(i));
            after.put("F3", String.valueOf(i));
            message.put("after", after);
            String msgStr = JSONObject.toJSONString(message, SerializerFeature.WRITE_MAP_NULL_FEATURES);
            KafkaDemo.sendMessage(producer, topic, String.valueOf(i), msgStr);
        }
        producer.flush();

    }

    @Test
    public void testKafkaProducer() {
        String topic = "FLINK_TEST_DJT";
        Producer<String, String> producer = KafkaDemo.createProducer(props);
        String timeStr = "2021-07-01 00:00:00";
        LocalDateTime time = LocalDateTime.parse(timeStr, DatePattern.NORM_DATETIME_FORMATTER);
        String[] names = {"刘一", "陈二", "张三", "李四", "王五", "赵六", "孙七", "周八", "吴九", "郑十"};
        Random random = new Random();
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("F1", names[random.nextInt(names.length)]);
            jsonObject.put("F2", 1);
            jsonObject.put("F3", time.plusSeconds(i).format(DjtConstant.YMDHMS_FORMAT));
            String msgStr = JSONObject.toJSONString(jsonObject, SerializerFeature.WRITE_MAP_NULL_FEATURES);
            KafkaDemo.sendMessage(producer, topic, String.valueOf(i), msgStr);
            ThreadUtil.sleep(100);
        }
        producer.flush();
    }

    @Test
    public void testKafkaConsumer() {
        String topic = "TEST_DJT";
        props.put("kafka.session.timeout.ms", "6000");
        props.put("kafka.heartbeat.interval.ms", "1000");
        //props.put("kafka.max.poll.interval.ms", "15000");
        props.put("kafka.max.poll.records", "100");
        KafkaDemo.startConsumer(props, 1000, 0, true, topic);
    }
}
