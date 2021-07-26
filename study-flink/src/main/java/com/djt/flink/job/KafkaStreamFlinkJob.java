package com.djt.flink.job;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.LocalDateTimeUtil;
import cn.hutool.setting.Setting;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Flink Kafka Source
 *
 * @author 　djt317@qq.com
 * @since 　 2021-07-15
 */
public class KafkaStreamFlinkJob extends AbsFlinkJob {

    public KafkaStreamFlinkJob(String jobName, Setting setting) {
        super(jobName, setting);
    }

    @Override
    protected void doRun(StreamExecutionEnvironment exeEnv) throws Exception {
        String topic = setting.get("kafka", "topic");
        Properties kafkaProps = getKafkaProps();
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), kafkaProps);
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
        //此方法已经弃用，默认就是 EventTime
        //exeEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置水印生成时间间隔
        exeEnv.getConfig().setAutoWatermarkInterval(1000);
        //设置并行度
        exeEnv.setParallelism(1);
        //exeEnv.enableCheckpointing(5000);
        DataStreamSource<String> kafkaStreamSource = exeEnv.addSource(kafkaConsumer);

        DataStream<String> ds = kafkaStreamSource.map(record -> {
            JSONObject json = JSON.parseObject(record);
            String f0 = json.getString("F1");
            Long f1 = json.getLongValue("F2");
            String f2 = json.getString("F3");
            return Tuple3.of(f0, f1, f2);
        }).returns(Types.TUPLE(Types.STRING, Types.LONG, Types.STRING))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        //.<Tuple3<String, Long, String>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .<Tuple3<String, Long, String>>forGenerator(new MyWatermarkGenerator<>(Duration.ofSeconds(5)))
                        .withTimestampAssigner((event, timestamp) ->
                                LocalDateTimeUtil.toEpochMilli(LocalDateTimeUtil.parse(event.f2, DatePattern.NORM_DATETIME_FORMATTER))))
                .keyBy(event -> event.f0)
                //窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //允许延迟时间
                .allowedLateness(Time.seconds(5))
                //数据处理
                .apply(new MyWindowFunction());
        ds.print();

        exeEnv.execute(jobName);
    }

    /**
     * 窗口函数
     * 触发时 打印相关信息
     */
    private static class MyWindowFunction implements WindowFunction<Tuple3<String, Long, String>, String, String, TimeWindow> {

        @Override
        public void apply(String s, TimeWindow window, Iterable<Tuple3<String, Long, String>> input, Collector<String> out) throws Exception {
            List<Tuple3<String, Long, String>> dataList = new ArrayList<>();
            for (Tuple3<String, Long, String> event : input) {
                dataList.add(event);
            }
            //dataList.sort((event1, event2) -> {
            //    Long ts1 = LocalDateTimeUtil.toEpochMilli(LocalDateTimeUtil.parse(event1.f2, DatePattern.NORM_DATETIME_FORMATTER));
            //    Long ts2 = LocalDateTimeUtil.toEpochMilli(LocalDateTimeUtil.parse(event2.f2, DatePattern.NORM_DATETIME_FORMATTER));
            //    return ts1.compareTo(ts2);
            //});

            //window.maxTimestamp()
            String msg = String.format("key:%s, 窗口:[%s--%s], 事件:[%s]",
                    s,
                    LocalDateTimeUtil.of(window.getStart()).format(DatePattern.NORM_DATETIME_FORMATTER),
                    LocalDateTimeUtil.of(window.getEnd()).format(DatePattern.NORM_DATETIME_FORMATTER),
                    StringUtils.join(dataList, ",")
            );
            out.collect(msg);
        }
    }

    /**
     * 自定义 Watermark 为了打印相关信息
     * 实际使用 forBoundedOutOfOrderness 即可
     *
     * @param <T>
     */
    private static class MyWatermark<T> implements WatermarkGenerator<T> {

        private long maxTimestamp;

        private final long outOfOrdernessMillis;

        public MyWatermark(Duration maxOutOfOrderness) {
            this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();
            this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis;
        }

        @Override
        public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
            maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
            System.out.printf("收到数据:%s 当前水印[%s]%n", event.toString(),
                    LocalDateTimeUtil.of(maxTimestamp - outOfOrdernessMillis)
                            .format(DatePattern.NORM_DATETIME_FORMATTER));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis));
        }
    }

    /**
     * 自定义 Watermark 生成器
     * 配合 MyWatermark 使用
     *
     * @param <T>
     */
    private static class MyWatermarkGenerator<T> implements WatermarkGeneratorSupplier<T> {

        private final Duration maxOutOfOrderness;

        public MyWatermarkGenerator(Duration maxOutOfOrderness) {
            checkNotNull(maxOutOfOrderness, "maxOutOfOrderness");
            checkArgument(!maxOutOfOrderness.isNegative(), "maxOutOfOrderness cannot be negative");
            this.maxOutOfOrderness = maxOutOfOrderness;
        }

        @Override
        public WatermarkGenerator<T> createWatermarkGenerator(Context context) {
            return new MyWatermark<>(maxOutOfOrderness);
        }
    }

}
