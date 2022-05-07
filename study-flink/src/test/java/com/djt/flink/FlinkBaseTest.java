package com.djt.flink;

import com.djt.event.MyEvent;
import com.djt.event.MySchema;
import com.djt.kafka.MyFlinkKafkaConsumer;
import com.djt.utils.ConfigConstants;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Flink测试基础类
 *
 * @author 　djt317@qq.com
 * @since 　 2021-07-23
 */
public class FlinkBaseTest {

    protected StreamExecutionEnvironment streamEnv = null;

    @Before
    public void before() {
        Configuration configuration = new Configuration();
        configuration.set(RestOptions.PORT, 8083);
        streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        streamEnv.setParallelism(ConfigConstants.flinkEnvParallelism());
        streamEnv.getConfig().setAutoWatermarkInterval(ConfigConstants.flinkWatermarkInterval());

        //streamEnv.setStateBackend(new FsStateBackend(ConfigConstants.flinkCheckpointPath()));
        //streamEnv.setStateBackend(new EmbeddedRocksDBStateBackend(true));

        CheckpointConfig checkpointConfig = streamEnv.getCheckpointConfig();
        checkpointConfig.enableUnalignedCheckpoints();
        checkpointConfig.setCheckpointStorage(ConfigConstants.flinkCheckpointPath());
        checkpointConfig.configure(ConfigConstants.getCheckpointConfig());
    }

    /**
     * 生成 Kafka DataStreamSource
     *
     * @return DataStream
     */
    public SingleOutputStreamOperator<MyEvent> getKafkaSourceWithWm() {
        Properties kafkaProps = ConfigConstants.getKafkaConsumerProps();
        String groupId = kafkaProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        String topic = ConfigConstants.topicEvent();
        return getKafkaSourceWithWm(topic, groupId);
    }


    public SingleOutputStreamOperator<MyEvent> getKafkaSourceWithWm(String topic, String groupId) {
        return getKafkaSourceWithWm(getKafkaSource(topic, groupId));
    }

    public static long outOrdTimeSec = 5;

    public SingleOutputStreamOperator<MyEvent> getKafkaSourceWithWm(DataStream<MyEvent> streamSource) {
        return streamSource
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<MyEvent>forBoundedOutOfOrderness(Duration.ofSeconds(outOrdTimeSec))
                        .withTimestampAssigner((event, timestamp) -> event.getEventTime()))
                .setParallelism(streamSource.getParallelism())
                .name("assignTimestampsAndWatermarks");
    }

    public SingleOutputStreamOperator<MyEvent> getKafkaSource() {
        Properties kafkaProps = ConfigConstants.getKafkaConsumerProps();
        String topic = ConfigConstants.topicEvent();
        String groupId = kafkaProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        return getKafkaSource(topic, groupId);
    }

    public SingleOutputStreamOperator<MyEvent> getKafkaSource(String topic, String groupId) {
        Properties kafkaProps = ConfigConstants.getKafkaConsumerProps();
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        FlinkKafkaConsumer<MyEvent> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new MySchema(), kafkaProps);
        return streamEnv.addSource(kafkaConsumer)
                .setParallelism(ConfigConstants.flinkSourceKafkaParallelism())
                .name("kafkaSource");
    }

    @Test
    public void testKafkaSource() throws Exception {
        SingleOutputStreamOperator<MyEvent> kafkaSource = getKafkaSourceWithWm();
        kafkaSource.print();
        streamEnv.execute("testKafkaSource");
    }

    @Test
    public void testKafkaSource2() throws Exception {
        Properties kafkaProps = ConfigConstants.getKafkaConsumerProps();
        String topic = ConfigConstants.topicEvent();

        //启动多个流，每个流只消费kafka的一个分区
        for (int i = 0; i < 3; i++) {
            MyFlinkKafkaConsumer<MyEvent> kafkaConsumer = new MyFlinkKafkaConsumer<>(topic, new MySchema(), kafkaProps);
            Map<KafkaTopicPartition, Long> startupOffsets = new HashMap<>();

            startupOffsets.put(new KafkaTopicPartition(topic, i), null);
            kafkaConsumer.setStartFromSpecificOffsets(startupOffsets);

            DataStream<MyEvent> kafkaSource = streamEnv.addSource(kafkaConsumer)
                    .setParallelism(1);
            kafkaSource.print().setParallelism(1);
        }

        streamEnv.execute("testKafkaSource2");
    }

    @Test
    public void testSocketSource() throws Exception {
        DataStreamSource<String> streamSource = streamEnv.addSource(
                new SocketTextStreamFunction("172.20.20.183", 10385, "\n", 10));
        streamSource.print().setParallelism(1);
        streamEnv.execute("testSocketSource");
    }

}
