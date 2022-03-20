package com.djt.tools.test;

import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.djt.utils.KafkaUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAdder;

import static com.djt.tools.AbsTools.PROPS;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-11-08
 */
public class OtherTest {

    public static volatile boolean isRunning = true;

    /**
     * 消费并打印指定数据
     */
    @Test
    public void startConsumer() {
        String topic = "RISK_ANALYSIS_STAT";
        PROPS.put("group.id", "flink_test");
        String merNo = "849501058120230";
        Properties properties = KafkaUtils.getConsumerProps(PROPS.toProperties());
        Producer<String, String> consumerTmp = KafkaUtils.createProducer(KafkaUtils.getConsumerProps(properties));
        int pts = consumerTmp.partitionsFor(topic).size();
        consumerTmp.close();
        ExecutorService pool = ThreadUtil.newExecutor(pts + 1, pts + 1, pts * 2);
        LongAdder counter = new LongAdder();
        for (int i = 0; i < pts; i++) {
            pool.execute(() -> {
                Consumer<String, String> consumer = KafkaUtils.createConsumer(KafkaUtils.getConsumerProps(properties));
                consumer.subscribe(Collections.singletonList(topic));
                while (isRunning) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    int rs = records.count();
                    counter.add(rs);
                    for (ConsumerRecord<String, String> record : records) {
                        JSONObject recordJson = JSON.parseObject(record.value());
                        String merNoStat = recordJson.getJSONObject("event").getString("merch_no");
                        if (!StringUtils.equals("*", merNo) && !merNo.equalsIgnoreCase(merNoStat)) {
                            continue;
                        }
                        System.out.println(StrUtil.format("消费数据=>topic:{} 商户号:{} 统计结果:{}",
                                record.topic(), merNoStat, recordJson.getJSONObject("ruleIds")));
                    }
                    consumer.commitSync();
                }
                consumer.close();
            });
        }

        pool.shutdown();
        while (!pool.isTerminated()) {
            ThreadUtil.sleep(1);
        }
    }
}
