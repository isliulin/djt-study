package com.djt.function;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 自定义触发器
 * ===============================
 * 由于 EventTimeTrigger 不能被继承
 * 为实现每来一条数据都触发一次计算
 * 因此重写该类并修改 onElement 方法
 * ===============================
 *
 * @author 　djt317@qq.com
 * @since 　 2021-07-27
 */
public class EveryEventTimeTrigger extends Trigger<Object, TimeWindow> {

    private EveryEventTimeTrigger() {}

    @Override
    public TriggerResult onElement(
            Object element, long timestamp, TimeWindow window, TriggerContext ctx) {
        if (ctx.getCurrentWatermark() <= window.maxTimestamp()) {
            ctx.registerEventTimeTimer(window.maxTimestamp());
        }
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
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) {
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
            ctx.registerEventTimeTimer(windowMaxTimestamp);
        }
    }

    @Override
    public String toString() {
        return "EveryEventTimeTrigger()";
    }

    public static EveryEventTimeTrigger create() {
        return new EveryEventTimeTrigger();
    }
}
