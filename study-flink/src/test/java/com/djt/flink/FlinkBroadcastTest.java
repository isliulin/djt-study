package com.djt.flink;

import cn.hutool.core.thread.ThreadUtil;
import com.djt.event.MyEvent;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Flink Broadcast 测试
 *
 * @author 　djt317@qq.com
 * @since 　 2021-07-23
 */
public class FlinkBroadcastTest extends FlinkBaseTest {

    private static final String[] names = {"刘一", "陈二", "张三", "李四", "王五", "赵六", "孙七", "周八", "吴九", "郑十"};
    private static final Map<String, OutputTag<MyEvent>> outputTagMap = new HashMap<>();

    @Test
    public void testBroadcast() throws Exception {
        streamEnv.getConfig().setAutoWatermarkInterval(1000);
        DataStream<MyEvent> kafkaSource = getKafkaSource();
        DataStreamSource<Map<String, Long>> riskRuleSource = streamEnv.addSource(new RiskRuleSourceFunction());
        MapStateDescriptor<String, Map<String, Long>> riskRuleMsd = new MapStateDescriptor<>(
                "riskRuleBdt",
                Types.STRING,
                new MapTypeInfo<>(Types.STRING, Types.LONG));
        BroadcastStream<Map<String, Long>> riskRuleBdt = riskRuleSource.setParallelism(1).broadcast(riskRuleMsd);
        for (String name : names) {
            outputTagMap.put(name, new OutputTag<MyEvent>(name) {});
        }

        SingleOutputStreamOperator<MyEvent> outputStream = kafkaSource.connect(riskRuleBdt)
                .process(new SplitProcessFunction(riskRuleMsd))
                .returns(TypeInformation.of(MyEvent.class));

        for (OutputTag<MyEvent> outputTag : outputTagMap.values()) {
            DataStream<MyEvent> stream = outputStream.getSideOutput(outputTag);
            stream.print();
        }

        streamEnv.execute("testBroadcast");
    }

    /**
     * 自定义数据源 模拟配置更新
     */
    public static class RiskRuleSourceFunction extends RichSourceFunction<Map<String, Long>> {

        private volatile boolean isRunning = true;
        private final Random random = new Random();
        private final Map<String, Long> config = new HashMap<>();

        @Override
        public void run(SourceContext<Map<String, Long>> ctx) {
            //每隔几秒 随机新增/更新/删除
            while (isRunning) {
                int flag = random.nextInt();
                if (flag % 3 > 0) {
                    long value = random.nextInt(100);
                    String name = names[random.nextInt(names.length)];
                    config.put(name, value);
                    System.out.println("新增/更新:" + name + "->" + value);
                } else {
                    String[] keys = config.keySet().toArray(new String[0]);
                    if (keys.length <= 0) continue;
                    String name = keys[random.nextInt(keys.length)];
                    config.remove(name);
                    System.out.println("删除:" + name);
                }
                ctx.collect(config);
                System.out.println("发布最新配置===============>" + config);
                ThreadUtil.sleep(10000);
            }
        }

        /**
         * 配置初始化 模拟程序启动时加载所有配置
         *
         * @param parameters ps
         * @throws Exception e
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            config.put(names[0], 10L);
            config.put(names[1], 10L);
            config.put(names[2], 10L);
            System.out.println("初始配置===============>" + config);
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    /**
     * 根据配置动态分流
     */
    public static class SplitProcessFunction extends BroadcastProcessFunction<MyEvent,
            Map<String, Long>, MyEvent> {

        private final MapStateDescriptor<String, Map<String, Long>> descriptor;

        public SplitProcessFunction(MapStateDescriptor<String, Map<String, Long>> descriptor) {
            this.descriptor = descriptor;
        }

        @Override
        public void processElement(MyEvent value, ReadOnlyContext ctx,
                                   Collector<MyEvent> out) throws Exception {
            Map<String, Long> config = ctx.getBroadcastState(descriptor).get("config");
            String datakey = value.getId();
            //将数据分流到对应输出 一条数据可能会输出到多个流 此处为方便起见 根据key判断 每条数据只会输出到一个流
            for (Map.Entry<String, Long> entry : config.entrySet()) {
                String confKey = entry.getKey();
                OutputTag<MyEvent> outputTag = outputTagMap.get(confKey);
                if (confKey.equals(datakey) && null != outputTag) {
                    ctx.output(outputTag, value);
                }
            }
        }

        @Override
        public void processBroadcastElement(Map<String, Long> value, Context ctx,
                                            Collector<MyEvent> out) throws Exception {
            System.out.println("收到最新配置===============>" + value.toString());
            ctx.getBroadcastState(descriptor).put("config", value);
        }
    }

}
