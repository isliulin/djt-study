package com.djt.function;

import cn.hutool.core.util.StrUtil;
import com.djt.utils.EventUtils;
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
    public TriggerResult onElement(
            Object element, long timestamp, TimeWindow window, TriggerContext ctx) {
        if (ctx.getCurrentWatermark() <= window.maxTimestamp()) {
            ctx.registerEventTimeTimer(window.maxTimestamp());
        }
        System.out.println(StrUtil.format("onElement触发===>{}", EventUtils.getTimeWindowStr(window)));
        return TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
        if (time == window.maxTimestamp()) {
            System.out.println(StrUtil.format("onEventTime触发===>{}", EventUtils.getTimeWindowStr(window)));
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
        //return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
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
        return "RiskEventTimeTrigger()";
    }

    public static MyEventTimeTrigger create() {
        return new MyEventTimeTrigger();
    }
}
