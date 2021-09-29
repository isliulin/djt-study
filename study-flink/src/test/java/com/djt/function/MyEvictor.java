package com.djt.function;

import com.djt.event.MyEvent;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import java.util.Iterator;

/**
 * 自定义 Evictor
 *
 * @author 　djt317@qq.com
 * @since 　 2021-09-28
 */
public class MyEvictor implements Evictor<MyEvent, TimeWindow> {

    @Override
    public void evictBefore(Iterable<TimestampedValue<MyEvent>> elements, int size,
                            TimeWindow window, EvictorContext evictorContext) {

    }

    @Override
    public void evictAfter(Iterable<TimestampedValue<MyEvent>> elements, int size,
                           TimeWindow window, EvictorContext evictorContext) {
        Iterator<TimestampedValue<MyEvent>> iter = elements.iterator();
        while (iter.hasNext()) {
            TimestampedValue<MyEvent> timestampedValue = iter.next();
            MyEvent event = timestampedValue.getValue();

            iter.remove();
        }
    }
}
