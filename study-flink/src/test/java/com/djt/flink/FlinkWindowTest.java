package com.djt.flink;

import com.djt.event.MyEvent;
import com.djt.function.EveryEventTimeTrigger;
import com.djt.function.MyWindowFunction;
import org.apache.flink.api.java.aggregation.AggregationFunctionFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.planner.plan.utils.AggFunctionFactory;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.time.temporal.ChronoField;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-08-27
 */
public class FlinkWindowTest extends FlinkBaseTest {

    @Test
    public void testTumblingEventTimeWindows() throws Exception {
        DataStream<MyEvent> kafkaSource = getKafkaSource();
        kafkaSource.keyBy(MyEvent::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(5))
                .trigger(EveryEventTimeTrigger.create())
                .apply(new MyWindowFunction());
        streamEnv.execute("testTumblingEventTimeWindows");
    }

    @Test
    public void testSlidingEventTimeWindows() throws Exception {
        DataStream<MyEvent> kafkaSource = getKafkaSource();
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
    public void testAggFunctionFactory() {
    }

}
