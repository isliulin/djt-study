package com.djt.function;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 自定义触发器
 *
 * @author 　djt317@qq.com
 * @since 　 2021-07-27
 */
public class MyEventTimeTrigger extends Trigger<Object, TimeWindow> {

    private MyEventTimeTrigger() {}

    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) {
        return TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) {
    }

    @Override
    public String toString() {
        return "MyEventTimeTrigger()";
    }

    public static MyEventTimeTrigger create() {
        return new MyEventTimeTrigger();
    }
}
