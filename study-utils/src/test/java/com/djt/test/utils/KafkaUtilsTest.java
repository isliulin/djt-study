package com.djt.test.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.djt.utils.KafkaUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-10-25
 */
public class KafkaUtilsTest {

    protected final Properties prop = new Properties();

    @Before
    public void before() {
        prop.put("bootstrap.servers", "172.20.4.83:9092,172.20.4.84:9092,172.20.4.85:9092");
    }

    @Test
    public void test1() {
        for (String key : ConsumerConfig.configNames()) {
            System.out.println(key);
        }
    }

    @Test
    public void testStartConsumer() {
        String topic = "RISK_ANALYSIS_STAT";
        prop.put("group.id", "flink_test");
        KafkaUtils.startConsumer(topic, 200, true, prop);
    }

    @Test
    public void testStartConsumer2() {
        String topic = "RISK_ANALYSIS_STAT";
        prop.put("group.id", "flink_test");
        String merNo = "849501058120230";
        Predicate<String> filterFunc = s -> {
            JSONObject recordJson = JSON.parseObject(s);
            String merNoTmp = recordJson.getJSONObject("event").getString("merch_no");
            return StringUtils.equals("*", merNo) || merNo.equalsIgnoreCase(merNoTmp);
        };
        Function<String, String> mapFunc = s -> {
            JSONObject msgJson = new JSONObject();
            JSONObject recordJson = JSON.parseObject(s);
            JSONObject eventJson = recordJson.getJSONObject("event");
            JSONObject statJson = recordJson.getJSONObject("ruleIds");
            String transTime = eventJson.getString("trans_time");
            msgJson.put("bbbbbbbbbbbbbbbbb", statJson.getString("bbbbbbbbbbbbbbbbb"));
            msgJson.put("merchCount", statJson.getString("merchCount"));
            msgJson.put("merchSuccessCount", statJson.getString("merchSuccessCount"));
            msgJson.put("merchQrCount", statJson.getString("merchQrCount"));
            msgJson.put("qrcodeSuccessCount", statJson.getString("qrcodeSuccessCount"));

            return "trans_time=" + transTime + " " + msgJson.toString();
        };

        KafkaUtils.startConsumer(topic, 200, true, prop, filterFunc, mapFunc);
    }

}
