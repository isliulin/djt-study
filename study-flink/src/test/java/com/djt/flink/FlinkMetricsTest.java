package com.djt.flink;

import com.djt.event.MyEvent;
import com.djt.function.EveryEventTimeTrigger;
import com.djt.function.MyWindowFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Test;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-11-05
 */
public class FlinkMetricsTest extends FlinkBaseTest {

    @Test
    public void testMetrics1() throws Exception {
        SingleOutputStreamOperator<MyEvent> kafkaSource = getKafkaSourceWithWm();
        kafkaSource.keyBy(MyEvent::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .trigger(EveryEventTimeTrigger.create())
                .apply(new MyWindowFunction());
        streamEnv.execute("testMetrics1");
    }
}
