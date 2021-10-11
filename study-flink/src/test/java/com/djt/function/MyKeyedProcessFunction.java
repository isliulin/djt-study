package com.djt.function;

import cn.hutool.core.util.StrUtil;
import com.djt.event.MyEvent;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
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
     * 字段收集结果
     */
    private AggregatingState<MyEvent, Set<String>> aggregatingState;

    /**
     * 过期时间
     */
    private ValueState<Long> lifeTimerState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.seconds(60))
                .updateTtlOnCreateAndWrite()
                .neverReturnExpired()
                .cleanupIncrementally(10, false)
                .build();

        AggregatingStateDescriptor<MyEvent, Set<String>, Set<String>> descriptor = new AggregatingStateDescriptor<>(
                "agg-join-state",
                new MyAggregateFunction(),
                TypeInformation.of(new TypeHint<Set<String>>() {}));
        descriptor.enableTimeToLive(stateTtlConfig);
        aggregatingState = getRuntimeContext().getAggregatingState(descriptor);

        ValueStateDescriptor<Long> lifeTimerDescriptor = new ValueStateDescriptor<>("lifeTimerState", Types.LONG);
        lifeTimerDescriptor.enableTimeToLive(stateTtlConfig);
        lifeTimerState = getRuntimeContext().getState(lifeTimerDescriptor);
    }

    @Override
    public void processElement(MyEvent value, Context ctx, Collector<Tuple2<String, Set<String>>> out) throws Exception {
        //System.out.println(StrUtil.format("processElement被调用===>currentProcessingTime={} value.getEventTime={} ctx.timestamp={}",
        //        ctx.timerService().currentProcessingTime(), value.getEventTime(), ctx.timestamp()));
        if (null == lifeTimerState.value()) {
            long newLifeTimer = ctx.timerService().currentProcessingTime() + 20000;
            ctx.timerService().registerProcessingTimeTimer(newLifeTimer);
            lifeTimerState.update(newLifeTimer);
        }

        aggregatingState.add(value);
        Set<String> values = aggregatingState.get();
        String key = ctx.getCurrentKey();
        if (values.size() >= 3) {
            System.out.println(StrUtil.format("key={}字段已凑齐", key));
            ctx.timerService().deleteProcessingTimeTimer(lifeTimerState.value());
            lifeTimerState.clear();
            aggregatingState.clear();
            out.collect(Tuple2.of(key, values));
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Set<String>>> out) throws Exception {
        //System.out.println(StrUtil.format("onTimer被调用===>timeDomain={} timestamp={} ctx.timestamp={}",
        //        ctx.timeDomain(), timestamp, ctx.timestamp()));
        Set<String> values = aggregatingState.get();
        String key = ctx.getCurrentKey();
        System.out.println(StrUtil.format("key={}过期时间到", key));
        out.collect(Tuple2.of(key, values));
        aggregatingState.clear();
        lifeTimerState.clear();
    }
}
