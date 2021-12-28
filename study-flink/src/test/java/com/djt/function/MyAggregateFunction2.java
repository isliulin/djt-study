package com.djt.function;

import com.djt.event.MyEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * 自定义 AggregateFunction
 *
 * @author 　djt317@qq.com
 * @since 　 2021-09-27
 */
public class MyAggregateFunction2 implements AggregateFunction<MyEvent, Long, Long> {

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
        return null;
    }
}
