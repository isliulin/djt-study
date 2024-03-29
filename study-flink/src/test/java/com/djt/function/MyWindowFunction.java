package com.djt.function;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.LocalDateTimeUtil;
import cn.hutool.core.util.StrUtil;
import com.djt.event.MyEvent;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * 自定义 WindowFunction
 *
 * @author 　djt317@qq.com
 * @since 　 2021-07-27
 */
@Log4j2
public class MyWindowFunction extends RichWindowFunction<MyEvent, MyEvent, String, TimeWindow> {

    @Override
    public void open(Configuration parameters) {
    }

    @Override
    public void apply(String s, TimeWindow window, Iterable<MyEvent> input, Collector<MyEvent> out) {
        String winStart = LocalDateTimeUtil.of(window.getStart()).format(DatePattern.NORM_DATETIME_FORMATTER);
        String winEnd = LocalDateTimeUtil.of(window.getEnd()).format(DatePattern.NORM_DATETIME_FORMATTER);

        List<MyEvent> eventList = new ArrayList<>();
        for (MyEvent event : input) {
            eventList.add(event);
            out.collect(event);
        }

        String msg = StrUtil.format("Key:{}, 窗口:[{}--{}], 数据:{}", s, winStart, winEnd, eventList.toString());
        System.out.println(msg);
    }

}