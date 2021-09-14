package com.djt.flink;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.LocalDateTimeUtil;
import com.djt.event.MyEvent;
import com.djt.function.EveryEventTimeTrigger;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

/**
 * Checkpoint测试
 *
 * @author 　djt317@qq.com
 * @since 　 2021-08-11
 */
public class FlinkCheckpointTest extends FlinkBaseTest {

    @Test
    public void testCheckpoint() throws Exception {
        streamEnv.getConfig().setAutoWatermarkInterval(1000);
        streamEnv.setParallelism(1);
        streamEnv.enableCheckpointing(60000);
        streamEnv.setStateBackend(new FsStateBackend("hdfs://nameserviceHA/user/flink/checkpoints/djt-study-flink"));
        CheckpointConfig checkpointConfig = streamEnv.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setCheckpointTimeout(60000);
        checkpointConfig.setMinPauseBetweenCheckpoints(60000);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.enableExternalizedCheckpoints(RETAIN_ON_CANCELLATION);
        checkpointConfig.setTolerableCheckpointFailureNumber(0);

        DataStream<MyEvent> kafkaSource = getKafkaSource();
        kafkaSource.keyBy(MyEvent::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(0))
                .trigger(EveryEventTimeTrigger.create())
                .aggregate(getAggregateFunction(), getWindowFunction());

        streamEnv.execute("testCheckpoint");
    }

    public AggregateFunction<MyEvent, Long, Long> getAggregateFunction() {
        return new AggregateFunction<MyEvent, Long, Long>() {
            @Override
            public Long createAccumulator() {
                return 0L;
            }

            @Override
            public Long add(MyEvent value, Long accumulator) {
                return accumulator + value.getNum();
            }

            @Override
            public Long getResult(Long accumulator) {
                return accumulator;
            }

            @Override
            public Long merge(Long a, Long b) {
                return a + b;
            }
        };
    }

    public WindowFunction<Long, Long, String, TimeWindow> getWindowFunction() {
        return new RichWindowFunction<Long, Long, String, TimeWindow>() {
            @Override
            public void apply(String key, TimeWindow window, Iterable<Long> input, Collector<Long> out) {
                long value = input.iterator().next();
                String msg = String.format("key:%s, 窗口:[%s--%s], value:%s", key,
                        LocalDateTimeUtil.of(window.getStart()).format(DatePattern.NORM_DATETIME_FORMATTER),
                        LocalDateTimeUtil.of(window.getEnd()).format(DatePattern.NORM_DATETIME_FORMATTER),
                        value);
                System.out.println(msg);
                out.collect(value);
            }
        };
    }
}
