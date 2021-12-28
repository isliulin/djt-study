package com.djt.function;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.LocalDateTimeUtil;
import cn.hutool.core.util.StrUtil;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 自定义 WindowFunction
 *
 * @author 　djt317@qq.com
 * @since 　 2021-07-27
 */
@Log4j2
public class MyAggWindowFunction2 implements WindowFunction<Long, String, String, TimeWindow> {

    @Override
    public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<String> out) {
        Long acc = input.iterator().next();
        String winStart = LocalDateTimeUtil.of(window.getStart()).format(DatePattern.NORM_DATETIME_FORMATTER);
        String winEnd = LocalDateTimeUtil.of(window.getEnd()).format(DatePattern.NORM_DATETIME_FORMATTER);
        out.collect(StrUtil.format("window[{} ~ {}] key={} value={}", winStart, winEnd, s, acc));
    }

}