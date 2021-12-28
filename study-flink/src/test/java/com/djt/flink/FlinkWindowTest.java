package com.djt.flink;

import com.djt.event.MyEvent;
import com.djt.function.*;
import com.djt.utils.ConfigConstants;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.junit.Test;

import java.time.temporal.ChronoField;
import java.util.Properties;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-08-27
 */
public class FlinkWindowTest extends FlinkBaseTest {

    @Test
    public void testTumblingEventTimeWindows() throws Exception {
        SingleOutputStreamOperator<MyEvent> kafkaSource = getKafkaSourceWithWm();
        kafkaSource.keyBy(MyEvent::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(5))
                .trigger(EveryEventTimeTrigger.create())
                .apply(new MyWindowFunction());
        streamEnv.execute("testTumblingEventTimeWindows");
    }

    @Test
    public void testSlidingEventTimeWindows() throws Exception {
        SingleOutputStreamOperator<MyEvent> kafkaSource = getKafkaSourceWithWm();
        int start = 23;
        int end = 6;
        int size = getWindowSizeHour(start, end);
        int offset = getWindowOffsetHour(start);
        kafkaSource.keyBy(MyEvent::getId)
                .window(SlidingEventTimeWindows.of(Time.hours(size), Time.hours(24), Time.hours(offset)))
                .allowedLateness(Time.seconds(5))
                .trigger(EveryEventTimeTrigger.create())
                .apply(new MyWindowFunction());
        streamEnv.execute("testSlidingEventTimeWindows");
    }


    @Test
    public void testWindowSizeHour() {
        System.out.println(getWindowSizeHour(0, 0));
        System.out.println(getWindowSizeHour(0, 8));
        System.out.println(getWindowSizeHour(23, 6));
    }

    @Test
    public void testWindowOffsetHour() {
        System.out.println(getWindowOffsetHour(0));
        System.out.println(getWindowOffsetHour(10));
        System.out.println(getWindowOffsetHour(23));
    }

    public static int getWindowSizeHour(int startHour, int endHour) {
        ChronoField.HOUR_OF_DAY.checkValidValue(startHour);
        ChronoField.HOUR_OF_DAY.checkValidValue(endHour);
        int gap = Math.abs(startHour - endHour);
        return endHour > startHour ? gap : 24 - gap;
    }

    public static int getWindowOffsetHour(int startHour) {
        ChronoField.HOUR_OF_DAY.checkValidValue(startHour);
        return startHour - 8;
    }

    @Test
    public void testKeyedProcessFunction() throws Exception {
        FlinkBaseTest.outOrdTimeSec = 1;
        SingleOutputStreamOperator<MyEvent> kafkaSource = getKafkaSourceWithWm();

        kafkaSource.keyBy(MyEvent::getId)
                .process(new MyKeyedProcessFunction());

        streamEnv.execute("testKeyedProcessFunction");
    }

    @Test
    public void testTumblingEventTimeWindows2() throws Exception {
        SingleOutputStreamOperator<MyEvent> kafkaSource = getKafkaSourceWithWm();

        kafkaSource.keyBy(MyEvent::getId)
                .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(16)))
                .trigger(EveryEventTimeTrigger.create())
                .apply(new MyWindowFunction());

        kafkaSource.keyBy(MyEvent::getId)
                .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(4)))
                .trigger(EveryEventTimeTrigger.create())
                .apply(new MyWindowFunction());

        streamEnv.execute("testKeyedProcessFunction");
    }

    @Test
    public void testTumblingEventTimeWindows3() throws Exception {
        SingleOutputStreamOperator<MyEvent> kafkaSource = getKafkaSourceWithWm();

        kafkaSource.keyBy(MyEvent::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .trigger(PurgingTrigger.of(MyEventTimeTrigger.create()))
                .apply(new MyWindowFunction());


        streamEnv.execute("testKeyedProcessFunction");
    }

    @Test
    public void testTumblingEventTimeWindows4() throws Exception {
        outOrdTimeSec = 2;
        SingleOutputStreamOperator<MyEvent> kafkaSource = getKafkaSourceWithWm();

        kafkaSource.keyBy(MyEvent::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new MyWindowFunction())
                .print();


        streamEnv.execute("testKeyedProcessFunction");
    }

    @Test
    public void testTumblingEventTimeWindows5() throws Exception {
        SingleOutputStreamOperator<MyEvent> kafkaSource = getKafkaSource();
        kafkaSource.keyBy(MyEvent::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new MyWindowFunction())
                .print();
        streamEnv.execute("testTumblingEventTimeWindows5");
    }

    @Test
    public void testWindowAgg() throws Exception {
        outOrdTimeSec = 60;
        Properties kafkaProps = ConfigConstants.getKafkaProducerProps();
        SingleOutputStreamOperator<MyEvent> kafkaSource = getKafkaSourceWithWm();
        kafkaSource
                .keyBy(MyEvent::getId)
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                .trigger(MyEventTimeTrigger.create())
                .allowedLateness(Time.minutes(1))
                .aggregate(new MyAggregateFunction2(), new MyAggWindowFunction2())
                .setParallelism(kafkaSource.getParallelism())
                .name("aggregate")
                .addSink(new FlinkKafkaProducer<>("flink-test-1", new SimpleStringSchema(), kafkaProps))
                .setParallelism(kafkaSource.getParallelism())
                .name("addSink_Kafka")
                .uid("addSink_Kafka");

        streamEnv.execute("testWindowAgg");
    }


}
