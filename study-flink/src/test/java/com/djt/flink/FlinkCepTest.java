package com.djt.flink;

import com.djt.event.MyEvent;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Flink CEP 测试
 *
 * @author 　djt317@qq.com
 * @since 　 2022-04-15
 */
public class FlinkCepTest extends FlinkBaseTest {

    @Test
    public void testCep1() throws Exception {
        SingleOutputStreamOperator<MyEvent> sourceStream = getKafkaSourceWithWm();
        sourceStream.print().setParallelism(1);

        Pattern<MyEvent, MyEvent> pattern = Pattern.<MyEvent>begin("first")
                .where(new SimpleCondition<MyEvent>() {
                    @Override
                    public boolean filter(MyEvent value) {
                        return StringUtils.equals(value.getName(), "张三");
                    }
                }).times(3).consecutive();

        PatternStream<MyEvent> patternStream = CEP.pattern(sourceStream, pattern);
        patternStream.select((PatternSelectFunction<MyEvent, String>) patternMap -> {
            List<MyEvent> eventList = patternMap.get("first");
            String ids = eventList.stream().map(MyEvent::getId).collect(Collectors.joining(" , "));
            return "连续3次输入张三: " + ids;
        }).print().setParallelism(1);

        streamEnv.execute("testCep1");
    }

    @Test
    public void testCep2() throws Exception {
        SingleOutputStreamOperator<String> sourceStream = streamEnv.addSource(
                new SocketTextStreamFunction("172.20.4.85", 6666, "\n", 10))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<String>forBoundedOutOfOrderness(Duration.ofSeconds(outOrdTimeSec))
                        .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis()))
                .setParallelism(1);
        sourceStream.print().setParallelism(1);

        Pattern<String, String> pattern = Pattern.<String>begin("start", AfterMatchSkipStrategy.noSkip())
                .where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String value) {
                        return value.startsWith("a");
                    }
                }).followedBy("middle").where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String value) {
                        return value.startsWith("b");
                    }
                }).oneOrMore().consecutive().followedBy("end1").where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String value) {
                        return value.startsWith("c");
                    }
                });

        PatternStream<String> patternStream = CEP.pattern(sourceStream, pattern);
        patternStream.select((PatternSelectFunction<String, String>) pattern1 -> {
            List<String> valueList = pattern1.get("next");
            return String.join(",", valueList);
        }).print().setParallelism(1);

        streamEnv.execute("testCep2");
    }

}
