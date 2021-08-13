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
        for (int i = 0; i < 1; i++) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("F1", "刘德华");
            jsonObject.put("F2", 1);
            jsonObject.put("F3", time.plusSeconds(i).format(DjtConstant.YMDHMS_FORMAT));
            String msgStr = JSONObject.toJSONString(jsonObject, SerializerFeature.WRITE_MAP_NULL_FEATURES);
            KafkaDemo.sendMessage(producer, topic, String.valueOf(i), msgStr);
            ThreadUtil.sleep(1000);
        }
        producer.flush();
    }

    @Test
    public void testKafkaProducerFlink() {
        String topic = "RISK_ANALYSIS_EVENT";
        Producer<String, String> producer = KafkaDemo.createProducer(props);
        String timeStr = "2021-07-01 12:34:56";
        LocalDateTime startTime = LocalDateTime.parse(timeStr, DatePattern.NORM_DATETIME_FORMATTER);
        String[] merNoArr = {"M0001", "M0002", "M0003", "M0004", "M0005", "M0006", "M0007", "M0008", "M0009", "M0010"};
        String[] nameArr = {"刘一", "陈二", "张三", "李四", "王五", "赵六", "孙七", "周八", "吴九", "郑十"};
        String[] retCodeArr = {"00", "01", "02", "03", "04", "05", "06", "55", "75", "38"};
        Random random = new Random();
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            LocalDateTime thisTime = startTime.plusMinutes(i);
            int merIndex = random.nextInt(merNoArr.length);
            String merNo = merNoArr[merIndex];
            String merName = nameArr[merIndex];
            String retCode = retCodeArr[random.nextInt(retCodeArr.length)];

            long timestamp = System.currentTimeMillis();
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("type", "03");
            jsonObject.put("subject", "test");
            jsonObject.put("timestamp", timestamp);
            jsonObject.put("event_id", timestamp);

            JSONObject event = new JSONObject();
            event.put("ORDER_ID", System.currentTimeMillis());
            event.put("ORI_ORDER_ID", "");
            event.put("BUSI_TYPE", "1001");
            event.put("OUT_ORDER_ID", "");
            event.put("MERCH_NO", merNo);
            event.put("TERM_NO", "456789");
            event.put("TERM_SN", "888888");
            event.put("PRINT_MERCH_NAME", merName);
            event.put("AGENT_ID", "50263545");
            event.put("SOURCES", "pos+/posp_api");
            event.put("TRANS_TIME", thisTime.format(DjtConstant.YMDHMS_FORMAT));
            event.put("AMOUNT", "100");
            event.put("STATUS", "2");
            event.put("EXPIRE_TIME", thisTime.plusSeconds(10).format(DjtConstant.YMDHMS_FORMAT));
            event.put("TRANS_TYPE", "SALE");
            event.put("PAY_TYPE", "payType");
            event.put("AREA_CODE", "440305");
            event.put("LOCATION", "192.168.10.6");
            event.put("FEE", "1");
            event.put("FEE_TYPE", "01");
            event.put("PAY_TOKEN", "379140010000021");
            event.put("RET_CODE", retCode);
            event.put("RET_MSG", "测试");
            event.put("AUTH_CODE", "132456");
            event.put("REMARK", "备注" + i);
            event.put("CREATE_TIME", thisTime.format(DjtConstant.YMDHMS_FORMAT));
            event.put("UPDATE_TIME", thisTime.format(DjtConstant.YMDHMS_FORMAT));

            jsonObject.put("event", event);
            System.out.println("======" + jsonObject.toJSONString());
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
