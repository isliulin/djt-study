package com.djt.flink;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.thread.ThreadUtil;
import com.alibaba.fastjson.JSON;
import com.djt.event.MyEvent;
import com.djt.event.MySchema;
import com.djt.utils.ConfigConstants;
import com.djt.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Properties;

/**
 * join 测试类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-09-26
 */
public class FlinkJoinTest extends FlinkBaseTest {

    @Test
    public void testJoin() throws Exception {
        DataStream<MyEvent> kafkaSource1 = getKafkaSource("flink-test-1", "group-flink-test-1");
        DataStream<MyEvent> kafkaSource2 = getKafkaSource("flink-test-2", "group-flink-test-2");
        DataStream<Tuple2<MyEvent, MyEvent>> joinStream = kafkaSource1.join(kafkaSource2)
                .where(MyEvent::getId)
                .equalTo(MyEvent::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(getJoinFunction());

        joinStream.print().setParallelism(1);

        streamEnv.execute("testJoin");
    }

    @Test
    public void makeSomeMyEvent() {
        makeSomeMyEventToKafka1();
        makeSomeMyEventToKafka2();
    }

    @Test
    public void makeSomeMyEventToKafka1() {
        String topic1 = "flink-test-1";
        //String topic2 = "flink-test-2";
        Producer<String, String> producer = KafkaUtils.createProducer(ConfigConstants.getKafkaProducerProps());
        LocalDateTime startTime = LocalDateTime.parse("2021-01-01 00:00:00", DatePattern.NORM_DATETIME_FORMATTER);
        for (int i = 0; i < 20; i++) {
            MyEvent event = new MyEvent();
            event.setId(String.valueOf(i));
            event.setName("张三_" + i);
            event.setNum(100L);
            event.setTime(startTime.plusSeconds(i).format(DatePattern.NORM_DATETIME_FORMATTER));
            KafkaUtils.sendMessage(producer, topic1, event.getId(), JSON.toJSONString(event));
            //KafkaUtils.sendMessage(producer, topic2, event.getId(), JSON.toJSONString(event));
            ThreadUtil.sleep(10);
        }
        producer.flush();
    }

    @Test
    public void makeSomeMyEventToKafka2() {
        String topic1 = "flink-test-2";
        Producer<String, String> producer = KafkaUtils.createProducer(ConfigConstants.getKafkaProducerProps());
        LocalDateTime startTime = LocalDateTime.parse("2021-01-01 00:00:00", DatePattern.NORM_DATETIME_FORMATTER);
        for (int i = 0; i < 20; i++) {
            MyEvent event = new MyEvent();
            event.setId(String.valueOf(i));
            event.setName("李四_" + i);
            event.setNum(100L);
            event.setTime(startTime.plusSeconds(i).format(DatePattern.NORM_DATETIME_FORMATTER));
            KafkaUtils.sendMessage(producer, topic1, event.getId(), JSON.toJSONString(event));
            ThreadUtil.sleep(10);
        }
        producer.flush();
    }

    public JoinFunction<MyEvent, MyEvent, Tuple2<MyEvent, MyEvent>> getJoinFunction() {
        return new RichJoinFunction<MyEvent, MyEvent, Tuple2<MyEvent, MyEvent>>() {
            @Override
            public Tuple2<MyEvent, MyEvent> join(MyEvent first, MyEvent second) {
                return Tuple2.of(first, second);
            }
        };
    }

    public DataStream<MyEvent> getKafkaSource(String topic, String groupId) {
        Properties kafkaProps = ConfigConstants.getKafkaConsumerProps();
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        FlinkKafkaConsumer<MyEvent> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new MySchema(), kafkaProps);
        return streamEnv.addSource(kafkaConsumer)
                .setParallelism(3)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<MyEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> event.getEventTime())
                        .withIdleness(Duration.ofMinutes(1)));
    }

}
