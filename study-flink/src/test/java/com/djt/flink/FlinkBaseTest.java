package com.djt.flink;

import com.djt.event.MyEvent;
import com.djt.event.MySchema;
import com.djt.utils.ConfigConstants;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
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
        configuration.set(RestOptions.PORT, 8082);
        streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        streamEnv.setParallelism(ConfigConstants.flinkEnvParallelism());
        streamEnv.getConfig().setAutoWatermarkInterval(ConfigConstants.flinkWatermarkInterval());
        streamEnv.setStateBackend(new FsStateBackend(ConfigConstants.flinkCheckpointPath()));
        CheckpointConfig checkpointConfig = streamEnv.getCheckpointConfig();
        checkpointConfig.configure(ConfigConstants.getCheckpointConfig());
    }

    /**
     * 生成 Kafka DataStreamSource
     *
     * @return DataStream
     */
    public DataStream<MyEvent> getKafkaSource() {
        Properties kafkaProps = ConfigConstants.getKafkaConsumerProps();
        String topic = ConfigConstants.topicEvent();
        FlinkKafkaConsumer<MyEvent> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new MySchema(), kafkaProps);
        return streamEnv.addSource(kafkaConsumer)
                .setParallelism(ConfigConstants.flinkSourceKafkaParallelism())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<MyEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> event.getEventTime())
                        .withIdleness(Duration.ofMinutes(1)));
    }


    @Test
    public void testKafkaSource() throws Exception {
        DataStream<MyEvent> kafkaSource = getKafkaSource();
        kafkaSource.print();
        streamEnv.execute("testKafkaSource");
    }

}
