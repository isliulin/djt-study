package com.djt.function;

import cn.hutool.core.util.StrUtil;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Set;

/**
 * 自定义 WindowFunction
 *
 * @author 　djt317@qq.com
 * @since 　 2021-07-27
 */
@Log4j2
public class MyAggWindowFunction implements WindowFunction<Set<String>, Tuple2<String, Set<String>>, String, TimeWindow> {

    @Override
    public void apply(String s, TimeWindow window, Iterable<Set<String>> input, Collector<Tuple2<String, Set<String>>> out) {
        Set<String> nameSet = input.iterator().next();
        if (nameSet.size() >= 3) {
            System.out.println(StrUtil.format("满足条件===>{}:", s));
            out.collect(Tuple2.of(s, nameSet));
        }

    }

}