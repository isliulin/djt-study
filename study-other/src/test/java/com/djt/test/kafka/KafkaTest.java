package com.djt.test.kafka;

import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.setting.dialect.PropsUtil;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.djt.kafka.KafkaDemo;
import com.djt.utils.DjtConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Properties;

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
        String topic = "dc_etl_baseinfo";
        String jsonStr = "{\n" +
                "  \"table\": \"BASE_DATA_USER.T_DICT\",\n" +
                "  \"op_type\": \"I\",\n" +
                "  \"op_ts\": \"2021-06-07 11:38:25.000000\",\n" +
                "  \"current_ts\": \"2021-06-07T19:38:29.242000\",\n" +
                "  \"pos\": \"00000001590007717741\",\n" +
                "  \"after\": {\n" +
                "    \"ID\": \"119670987\",\n" +
                "    \"VALUE\": \"1\",\n" +
                "    \"VALUE_NAME\": \"VALUE_NAME\",\n" +
                "    \"LABEL\": \"LABEL\",\n" +
                "    \"TYPE\": \"TYPE\",\n" +
                "    \"TYPE_NAME\": \"TYPE_NAME\",\n" +
                "    \"PROJECT\": \"PROJECT\",\n" +
                "    \"REMARK\": \"REMARK\",\n" +
                "    \"SORT\": \"SORT\",\n" +
                "    \"PARENT_ID\": \"PARENT_ID\",\n" +
                "    \"CREATE_TIME\": \"2021-06-06 11:38:25.000000\",\n" +
                "    \"UPDATE_TIME\": \"2021-06-06 11:38:25.000000\",\n" +
                "    \"DEL_FLAG\": \"DEL_FLAG\"\n" +
                "  }\n" +
                "}";
        Producer<String, String> producer = KafkaDemo.createProducer(props);
        KafkaDemo.sendMessage(producer, topic, "0", jsonStr);
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
        String topic = "TEST_DJT";
        //String topic = "SPARK_ETL_DJT_1";
        Producer<String, String> producer = KafkaDemo.createProducer(props);
        for (int i = 0; i < 10000; i++) {
            //int topicNo = (i % 3) + 1;
            //topic = "SPARK_ETL_DJT_" + topicNo;
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("F1", i);
            jsonObject.put("F2", i);
            jsonObject.put("F3", i);
            String msgStr = JSONObject.toJSONString(jsonObject, SerializerFeature.WRITE_MAP_NULL_FEATURES);
            KafkaDemo.sendMessage(producer, topic, String.valueOf(i), msgStr);
            ThreadUtil.sleep(1000);
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
