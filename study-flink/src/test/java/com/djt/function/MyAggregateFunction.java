package com.djt.function;

import com.djt.event.MyEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashSet;
import java.util.Set;

/**
 * 自定义 AggregateFunction
 *
 * @author 　djt317@qq.com
 * @since 　 2021-09-27
 */
public class MyAggregateFunction implements AggregateFunction<MyEvent, Set<String>, Set<String>> {

    @Override
    public Set<String> createAccumulator() {
        return new HashSet<>();
    }

    @Override
    public Set<String> add(MyEvent value, Set<String> accumulator) {
        accumulator.add(value.getName());
        return accumulator;
    }

    @Override
    public Set<String> getResult(Set<String> accumulator) {
        return accumulator;
    }

    @Override
    public Set<String> merge(Set<String> a, Set<String> b) {
        a.addAll(b);
        return a;
    }
}
