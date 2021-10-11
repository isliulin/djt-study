package com.djt.flink;

import cn.hutool.core.util.StrUtil;
import com.djt.event.MyEvent;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.IdPartitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.Test;

/**
 * @author 　djt317@qq.com
 * @since 　 2021-09-03
 */
public class FlinkPartitionTest extends FlinkBaseTest {

    @Test
    public void testPartition() throws Exception {
        DataStream<MyEvent> kafkaSource = getKafkaSourceWithWm();
        kafkaSource.partitionCustom(new MyPartition(), MyEvent::getId).print().setParallelism(7);

        streamEnv.execute("testPartition");
    }

    @Test
    public void testPartition2() throws Exception {
        DataStream<MyEvent> kafkaSource = getKafkaSourceWithWm();
        kafkaSource.partitionCustom(new IdPartitioner(), (KeySelector<MyEvent, Integer>) value -> 7).print().setParallelism(5);

        streamEnv.execute("testPartition2");
    }

    public static class MyPartition implements Partitioner<String> {

        @Override
        public int partition(String key, int numPartitions) {
            int p = Math.abs(key.hashCode()) % numPartitions;
            System.out.println(StrUtil.format("分区总数={} 当前key={} 发送分区={}", numPartitions, key, p));
            return p;
        }
    }
}
