package com.djt.flink;

import cn.hutool.core.util.StrUtil;
import com.djt.event.MyEvent;
import com.djt.function.EveryEventTimeTrigger;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * aggregate 测试类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-10-27
 */
public class FlinkAggTest extends FlinkBaseTest {


    @Test
    public void testAgg1() throws Exception {
        Map<String, AggregateFunction<MyEvent, ?, ?>> funcMap = new HashMap<>();
        funcMap.put("AAA", new Agg1());
        funcMap.put("BBB", new Agg2());
        funcMap.put("CCC", new Agg3());

        SingleOutputStreamOperator<MyEvent> sourceStream = getKafkaSourceWithWm();
        sourceStream
                .keyBy(MyEvent::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .trigger(EveryEventTimeTrigger.create())
                .aggregate(new MyMultiAggregateFunction(funcMap))  //, new MyMultiAggWindowFunction()
                .print();

        streamEnv.execute("testAgg1");
    }


    public static class MyMultiAggregateFunction implements AggregateFunction<MyEvent,
            Map<String, Object>, Map<String, Object>> {

        private final Map<String, AggregateFunction<MyEvent, ?, ?>> funcMap;

        public MyMultiAggregateFunction(Map<String, AggregateFunction<MyEvent, ?, ?>> funcMap) {
            this.funcMap = funcMap;
        }

        @Override
        public Map<String, Object> createAccumulator() {
            Map<String, Object> accMap = new HashMap<>(1);
            funcMap.forEach((key, func) -> accMap.put(key, func.createAccumulator()));
            return accMap;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Map<String, Object> add(MyEvent value, Map<String, Object> accumulator) {
            accumulator.forEach((key, acc) -> {
                AggregateFunction<MyEvent, Object, Object> func =
                        (AggregateFunction<MyEvent, Object, Object>) funcMap.get(key);
                Object newAcc = func.add(value, acc);
                accumulator.put(key, newAcc);
            });
            return accumulator;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Map<String, Object> getResult(Map<String, Object> accumulator) {
            Map<String, Object> result = new HashMap<>(1);
            accumulator.forEach((key, acc) -> {
                AggregateFunction<MyEvent, Object, Object> func =
                        (AggregateFunction<MyEvent, Object, Object>) funcMap.get(key);
                result.put(key, func.getResult(acc));
            });
            return result;
        }

        @Override
        public Map<String, Object> merge(Map<String, Object> a, Map<String, Object> b) {
            return null;
        }
    }

    public static class MyMultiAggWindowFunction
            implements WindowFunction<Map<String, Object>, Map<String, Object>, String, TimeWindow> {

        @Override
        public void apply(String s, TimeWindow window, Iterable<Map<String, Object>> input, Collector<Map<String, Object>> out) {
            Map<String, Object> resultMap = input.iterator().next();
            System.out.println(StrUtil.format("==========key={}==========", s));
            for (Map.Entry<String, Object> entry : resultMap.entrySet()) {
                System.out.println(StrUtil.format("agg={} value={}", entry.getKey(), entry.getValue()));
            }
        }

    }

    public static class Agg1 implements AggregateFunction<MyEvent, Long, Long> {

        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(MyEvent value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    public static class Agg2 implements AggregateFunction<MyEvent, Long, Long> {
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
    }

    public static class Agg3 implements AggregateFunction<MyEvent, Tuple2<Long, Long>, Double> {
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return Tuple2.of(0L, 0L);
        }

        @Override
        public Tuple2<Long, Long> add(MyEvent value, Tuple2<Long, Long> accumulator) {
            accumulator.f0 += value.getNum();
            accumulator.f1 += 1;
            return accumulator;
        }

        @Override
        public Double getResult(Tuple2<Long, Long> accumulator) {
            return accumulator.f0 * 1.0 / accumulator.f1;
        }

        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
            return null;
        }
    }


}
