package com.djt.tools.test;

import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.djt.utils.KafkaUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAdder;

import static com.djt.tools.AbsTools.PROPS;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-11-08
 */
public class OtherTest {

    @Test
    public void testOrc1() {
        Path path = new Path("C:\\Users\\duanjiatao\\Desktop\\tmp\\testData\\orc-001");
        Reader reader;
        try {
            reader = OrcFile.createReader(path, OrcFile.readerOptions(new Configuration()));
            StructObjectInspector inspector = (StructObjectInspector) reader.getObjectInspector();
            RecordReader records = reader.rows();
            Object row = null;
            List<? extends StructField> structFields = inspector.getAllStructFieldRefs();
            while (records.hasNext()) {
                row = records.next(row);
                JSONObject jsonObject = new JSONObject();
                for (StructField structField : structFields) {
                    String name = structField.getFieldName();
                    Object value = inspector.getStructFieldData(row, structField);
                    value = value == null ? null : value.toString();
                    jsonObject.put(name, value);
                }
                System.out.println(jsonObject.toString());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

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
