package com.djt.test.kafka;

import com.alibaba.druid.pool.ha.PropertiesUtils;
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
        props = PropertiesUtils.loadProperties("/config.properties");
    }

    @Test
    public void testKafkaProducer() throws InterruptedException {
        String topic = "TEST_DJT";
        String table = "test.t_test_djt";
        //KafkaDemo.startConsumer(props, topic);
        Producer<String, String> producer = KafkaDemo.createProducer(props);
        for (int i = 0; i < 1; i++) {
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
            after.put("F1", "1");
            after.put("F2", "111");
            after.put("F3", "111");
            message.put("after", after);
            String msgStr = JSONObject.toJSONString(message, SerializerFeature.WRITE_MAP_NULL_FEATURES);
            KafkaDemo.sendMessage(producer, topic, String.valueOf(i), msgStr);
            Thread.sleep(1000);
        }

    }

    @Test
    public void testKafkaConsumer() {

    }
}
