package com.djt.function;

import com.djt.event.MyEvent;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 自定义 SourceFunction
 *
 * @author 　djt317@qq.com
 * @since 　 2021-09-26
 */
public class MyEventSource implements SourceFunction<MyEvent> {



    @Override
    public void run(SourceContext<MyEvent> ctx) {

    }

    @Override
    public void cancel() {}

}
