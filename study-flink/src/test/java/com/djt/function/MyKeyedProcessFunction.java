package com.djt.function;

import com.djt.event.MyEvent;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Set;

/**
 * 自定义 KeyedProcessFunction
 *
 * @author 　djt317@qq.com
 * @since 　 2021-09-27
 */
public class MyKeyedProcessFunction extends KeyedProcessFunction<String, MyEvent, Tuple2<String, Set<String>>> {

    /**
     * 聚合结果 State
     */
    private transient AggregatingState<MyEvent, Set<String>> aggregatingState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.seconds(10))
                .updateTtlOnCreateAndWrite()
                .neverReturnExpired()
                .cleanupIncrementally(10, false)
                .build();

        AggregatingStateDescriptor<MyEvent, Set<String>, Set<String>> descriptor = new AggregatingStateDescriptor<>(
                "descriptor-join-state",
                new MyAggregateFunction(),
                TypeInformation.of(new TypeHint<Set<String>>() {}));
        descriptor.enableTimeToLive(stateTtlConfig);
        aggregatingState = getRuntimeContext().getAggregatingState(descriptor);
    }

    @Override
    public void processElement(MyEvent value, Context ctx, Collector<Tuple2<String, Set<String>>> out) throws Exception {
        aggregatingState.add(value);
        Set<String> values = aggregatingState.get();
        String key = ctx.getCurrentKey();
        out.collect(Tuple2.of(key, values));
    }
}
